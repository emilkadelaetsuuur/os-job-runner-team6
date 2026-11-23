/* server.c -- the job runner's server.
 *
 *   ./server <port>
 *
 * One listening socket for everybody. The first line on a connection
 * decides what it is: "HELLO <K>" is a worker, anything else is a client.
 * Every connection gets its own thread, so a silent client never blocks
 * the others. All job records live in one table protected by one mutex.
 *
 * Durability (Part 2): every change of the table is first written to the
 * journal jobs.log (journal_append() waits for fsync), and only then made
 * and answered. Two kinds of line:
 *     SUBMIT T12-7 <timeout> <priority> <escaped command>
 *     STATE T12-7 DONE 0          (the same text status prints)
 * At start the server replays jobs.log with apply(), which only changes the
 * table and never writes. Jobs that were RUNNING go back to QUEUED.
 * A job's output is kept in out/<id>, written before its final STATE line.
 *
 * Scheduling policy (environment variable POLICY):
 *   priority  (default) highest priority first, with aging: every AGING
 *             seconds spent QUEUED adds one level, so nobody waits forever.
 *   fifo      oldest job first.
 */
#define _GNU_SOURCE
#include "common.h"
#include "journal.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#define JOURNAL_PATH "jobs.log"
#define OUT_DIR      "out"

/* ------------------------------------------------------------------ */
/* Data                                                               */
/* ------------------------------------------------------------------ */

typedef enum {
    ST_QUEUED,
    ST_RUNNING,
    ST_DONE,              /* code = exit status   */
    ST_KILLED_TIMEOUT,
    ST_KILLED_SIGNAL,     /* code = signal number */
    ST_CANCELLED
} state_t;

typedef struct worker {
    int            id;
    int            fd;
    int            slots;     /* K, announced in HELLO       */
    int            free;      /* slots not holding a job     */
    int            alive;     /* 0 once a write to it failed */
    struct worker *next;
} worker_t;

typedef struct {
    int       num;            /* the id is T<TEAM>-<num> */
    char     *cmd;            /* escaped, exactly as on the wire */
    int       timeout;        /* seconds */
    int       priority;       /* 0..9, 9 highest */
    state_t   state;
    int       code;
    long long submitted;      /* now_ms(); after a restart: the replay time */
    worker_t *worker;         /* worker holding a slot for this job, or NULL */
} job_t;

static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  reported = PTHREAD_COND_INITIALIZER;

static job_t   **jobs;        /* jobs[n-1] is T<TEAM>-n */
static int       njobs, jobs_cap;
static job_t   **queue;       /* QUEUED jobs, in submission order */
static int       nqueue, queue_cap;
static worker_t *workers;
static int       next_worker_id = 1;
static int       journal_fd = -1;

static enum { POL_FIFO, POL_PRIORITY } policy = POL_PRIORITY;
static int aging_sec = 5;

static int sig_pipe[2] = { -1, -1 };   /* signal handler -> main loop */

/* ------------------------------------------------------------------ */
/* Logging                                                            */
/* ------------------------------------------------------------------ */

static void logmsg(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
static void logmsg(const char *fmt, ...)
{
    va_list ap;
    flockfile(stdout);
    fputs("server: ", stdout);
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    putchar('\n');
    fflush(stdout);
    funlockfile(stdout);
}

/* ------------------------------------------------------------------ */
/* The job table. Every function here expects the lock to be held.   */
/* ------------------------------------------------------------------ */

static int is_final(state_t s)
{
    return s != ST_QUEUED && s != ST_RUNNING;
}

/* "T12-7 DONE 0": the exact text a client prints for status,
 * and the body of a STATE line in the journal. */
static void format_status(const job_t *j, char *buf, size_t cap)
{
    switch (j->state) {
    case ST_QUEUED:         snprintf(buf, cap, "T%d-%d QUEUED", TEAM, j->num); break;
    case ST_RUNNING:        snprintf(buf, cap, "T%d-%d RUNNING", TEAM, j->num); break;
    case ST_DONE:           snprintf(buf, cap, "T%d-%d DONE %d", TEAM, j->num, j->code); break;
    case ST_KILLED_TIMEOUT: snprintf(buf, cap, "T%d-%d KILLED TIMEOUT", TEAM, j->num); break;
    case ST_KILLED_SIGNAL:  snprintf(buf, cap, "T%d-%d KILLED SIGNAL %d", TEAM, j->num, j->code); break;
    case ST_CANCELLED:      snprintf(buf, cap, "T%d-%d CANCELLED", TEAM, j->num); break;
    }
}

static void out_path(int num, char *buf, size_t cap)
{
    snprintf(buf, cap, OUT_DIR "/T%d-%d", TEAM, num);
}

static void queue_add(job_t *j)
{
    if (nqueue == queue_cap) {
        queue_cap = queue_cap ? queue_cap * 2 : 64;
        queue = realloc(queue, (size_t)queue_cap * sizeof *queue);
        if (!queue) { perror("server: realloc"); exit(1); }
    }
    /* keep submission order, also for jobs put back after a worker left */
    int i = nqueue++;
    while (i > 0 && queue[i - 1]->num > j->num) {
        queue[i] = queue[i - 1];
        i--;
    }
    queue[i] = j;
}

static void queue_remove(job_t *j)
{
    for (int i = 0; i < nqueue; i++)
        if (queue[i] == j) {
            memmove(&queue[i], &queue[i + 1], (size_t)(nqueue - i - 1) * sizeof *queue);
            nqueue--;
            return;
        }
}

/* "T12-7" -> job, or NULL if this server never issued that id. */
static job_t *job_find(const char *id)
{
    char prefix[32];
    long n;
    int plen = snprintf(prefix, sizeof prefix, "T%d-", TEAM);
    if (strncmp(id, prefix, (size_t)plen) != 0)
        return NULL;
    /* exactly the digits we issued: no sign, no spaces, no leading zero */
    if (id[plen] < '1' || id[plen] > '9' || parse_int(id + plen, 1, njobs, &n) < 0)
        return NULL;
    return jobs[n - 1];
}

/* --- apply: the table changes, and nothing else. Never journals. --- */

static void apply_state(job_t *j, state_t s, int code)
{
    if (j->state == ST_QUEUED && s != ST_QUEUED)
        queue_remove(j);
    if (j->state != ST_QUEUED && s == ST_QUEUED) {
        j->submitted = now_ms();      /* waiting starts again: aging counts from now */
        queue_add(j);
    }
    j->state = s;
    j->code = code;
}

static job_t *apply_submit(int num, int timeout, int priority, const char *cmd)
{
    if (num != njobs + 1)
        return NULL;                  /* ids are issued in order, never twice */
    if (njobs == jobs_cap) {
        jobs_cap = jobs_cap ? jobs_cap * 2 : 64;
        jobs = realloc(jobs, (size_t)jobs_cap * sizeof *jobs);
        if (!jobs) { perror("server: realloc"); exit(1); }
    }
    job_t *j = calloc(1, sizeof *j);
    if (!j || !(j->cmd = strdup(cmd))) { perror("server: malloc"); exit(1); }
    j->num = num;
    j->timeout = timeout;
    j->priority = priority;
    j->state = ST_QUEUED;
    j->submitted = now_ms();
    jobs[njobs++] = j;
    queue_add(j);
    return j;
}

/* One journal line -> one change of the table. Unknown lines are skipped. */
static int apply(const char *line)
{
    char id[64], w1[16] = "", w2[16] = "";
    int num, timeout, priority, code = 0, used = 0;

    if (sscanf(line, "SUBMIT T%*d-%d %d %d %n", &num, &timeout, &priority, &used) == 3 &&
        used > 0 && line[used] != '\0')
        return apply_submit(num, timeout, priority, line + used) ? 0 : -1;

    if (sscanf(line, "STATE %63s %15s %15s %d", id, w1, w2, &code) >= 2) {
        job_t *j = job_find(id);
        if (!j)
            return -1;
        if (strcmp(w1, "QUEUED") == 0)        apply_state(j, ST_QUEUED, 0);
        else if (strcmp(w1, "RUNNING") == 0)  apply_state(j, ST_RUNNING, 0);
        else if (strcmp(w1, "DONE") == 0)     apply_state(j, ST_DONE, atoi(w2));
        else if (strcmp(w1, "CANCELLED") == 0) apply_state(j, ST_CANCELLED, 0);
        else if (strcmp(w1, "KILLED") == 0 && strcmp(w2, "TIMEOUT") == 0)
            apply_state(j, ST_KILLED_TIMEOUT, 0);
        else if (strcmp(w1, "KILLED") == 0 && strcmp(w2, "SIGNAL") == 0)
            apply_state(j, ST_KILLED_SIGNAL, code);
        else
            return -1;
        return 0;
    }
    return -1;
}

/* --- journal first, then apply: the live path --- */

/* The ONLY place a job's state changes while the server runs. */
static void job_set_state(job_t *j, state_t s, int code)
{
    char before[64], after[64], line[96];
    format_status(j, before, sizeof before);

    job_t next = *j;
    next.state = s;
    next.code = code;
    format_status(&next, after, sizeof after);
    snprintf(line, sizeof line, "STATE %s", after);

    if (journal_append(journal_fd, line) < 0)
        logmsg("WARNING: journal write failed for \"%s\": %s", line, strerror(errno));
    apply_state(j, s, code);

    logmsg("state %s -> %s", before, after + strcspn(after, " ") + 1);
}

/* ------------------------------------------------------------------ */
/* Scheduling                                                         */
/* ------------------------------------------------------------------ */

static int effective_priority(const job_t *j, long long now)
{
    int p = j->priority + (int)((now - j->submitted) / 1000 / aging_sec);
    return p > 9 ? 9 : p;
}

/* The policy: which QUEUED job runs next. */
static job_t *policy_next(void)
{
    if (nqueue == 0)
        return NULL;
    if (policy == POL_FIFO)
        return queue[0];

    long long now = now_ms();
    job_t *best = queue[0];
    int best_p = effective_priority(best, now);
    for (int i = 1; i < nqueue; i++) {
        int p = effective_priority(queue[i], now);
        if (p > best_p) {          /* strict '>': ties go to the older job */
            best = queue[i];
            best_p = p;
        }
    }
    return best;
}

/* The worker with the most free slots, or NULL. */
static worker_t *pick_worker(void)
{
    worker_t *best = NULL;
    for (worker_t *w = workers; w; w = w->next)
        if (w->alive && w->free > 0 && (!best || w->free > best->free))
            best = w;
    return best;
}

/* While some worker has a free slot and some job waits: send one. */
static void dispatch(void)
{
    worker_t *w;
    job_t *j;
    while ((w = pick_worker()) != NULL && (j = policy_next()) != NULL) {
        long long now = now_ms();
        int eff = policy == POL_FIFO ? j->priority : effective_priority(j, now);
        long long waited = now - j->submitted;

        /* journal RUNNING first, then tell the worker */
        j->worker = w;
        w->free--;
        job_set_state(j, ST_RUNNING, 0);
        if (send_line(w->fd, "RUN T%d-%d %d %s", TEAM, j->num, j->timeout, j->cmd) < 0) {
            logmsg("worker %d: write failed, not using it any more", w->id);
            w->alive = 0;              /* its own thread cleans up on EOF */
            j->worker = NULL;
            w->free++;
            job_set_state(j, ST_QUEUED, 0);
            continue;
        }
        logmsg("dispatch T%d-%d -> worker %d (priority %d, effective %d, waited %lld ms, "
               "%d queued left, policy %s)",
               TEAM, j->num, w->id, j->priority, eff, waited, nqueue,
               policy == POL_FIFO ? "fifo" : "priority");
    }
}

/* ------------------------------------------------------------------ */
/* Clients                                                            */
/* ------------------------------------------------------------------ */

static void handle_submit(int fd, char *args)
{
    /* args = "<timeout> <priority> <escaped command>" */
    char *t = strtok_r(args, " ", &args);
    char *p = strtok_r(NULL, " ", &args);
    char *cmd = args;
    long timeout, prio;

    if (!t || !p || parse_int(t, 1, 1000000000L, &timeout) < 0 ||
        parse_int(p, 0, 9, &prio) < 0 || !cmd || *cmd == '\0' || strlen(cmd) >= CMD_MAX) {
        send_line(fd, "ERROR usage: SUBMIT <timeout> <priority> <command>");
        return;
    }

    char line[JOURNAL_LINE], path[64];
    pthread_mutex_lock(&lock);
    int num = njobs + 1;
    out_path(num, path, sizeof path);
    unlink(path);                      /* an old run's output must never show up */
    snprintf(line, sizeof line, "SUBMIT T%d-%d %ld %ld %s", TEAM, num, timeout, prio, cmd);

    /* write first, then answer: the id exists only once its line is on disk */
    if (journal_append(journal_fd, line) < 0) {
        pthread_mutex_unlock(&lock);
        logmsg("WARNING: journal write failed, refusing a submit: %s", strerror(errno));
        send_line(fd, "ERROR the server cannot write its journal");
        return;
    }
    apply_submit(num, (int)timeout, (int)prio, cmd);
    logmsg("job T%d-%d accepted: \"%s\" timeout %ld s, priority %ld",
           TEAM, num, cmd, timeout, prio);
    dispatch();
    pthread_mutex_unlock(&lock);

    send_line(fd, "ID T%d-%d", TEAM, num);
}

static void handle_status(int fd, const char *id)
{
    char text[64];
    pthread_mutex_lock(&lock);
    job_t *j = job_find(id);
    if (j)
        format_status(j, text, sizeof text);
    pthread_mutex_unlock(&lock);

    if (j)
        send_line(fd, "STATE %s", text);
    else
        send_line(fd, "UNKNOWN %s", id);
}

static void handle_cancel(int fd, const char *id)
{
    const char *answer = "UNKNOWN";
    pthread_mutex_lock(&lock);
    job_t *j = job_find(id);
    if (j && j->state == ST_QUEUED) {
        job_set_state(j, ST_CANCELLED, 0);
        answer = "CANCELLED";
    } else if (j && j->state == ST_RUNNING) {
        /* Only the worker can signal the job. The slot stays taken
         * until the worker reports the job gone. */
        job_set_state(j, ST_CANCELLED, 0);
        if (j->worker && j->worker->alive &&
            send_line(j->worker->fd, "CANCEL T%d-%d", TEAM, j->num) < 0)
            j->worker->alive = 0;
        answer = "CANCELLED";
    } else if (j) {
        answer = "FINISHED";
    }
    pthread_mutex_unlock(&lock);

    send_line(fd, "%s %s", answer, id);
}

static void handle_fetch(int fd, const char *id)
{
    int found, finished, num = 0;

    pthread_mutex_lock(&lock);
    job_t *j = job_find(id);
    found = j != NULL;
    if (j && is_final(j->state) && j->worker) {
        /* cancelled, but the worker has not reported yet: give it a moment
         * so fetch returns what the job actually printed */
        struct timespec until;
        clock_gettime(CLOCK_REALTIME, &until);
        until.tv_sec += 3;
        while (j->worker && pthread_cond_timedwait(&reported, &lock, &until) == 0)
            ;
    }
    finished = j && is_final(j->state);
    if (j)
        num = j->num;
    pthread_mutex_unlock(&lock);

    if (!found) {
        send_line(fd, "UNKNOWN %s", id);
        return;
    }
    if (!finished) {
        send_line(fd, "NOTFINISHED %s", id);
        return;
    }

    /* A final job's file never changes again, so no lock is needed to read it.
     * No file (e.g. cancelled while QUEUED): the job printed nothing. */
    char path[64];
    out_path(num, path, sizeof path);
    int ofd = open(path, O_RDONLY | O_CLOEXEC);
    struct stat st;
    size_t len = 0;
    if (ofd >= 0 && fstat(ofd, &st) == 0)
        len = (size_t)st.st_size;

    /* many lines, maybe no final '\n': length prefix, then the bytes */
    if (send_line(fd, "OUTPUT %zu", len) == 0) {
        char buf[8192];
        size_t left = len;
        while (left > 0) {
            ssize_t n = read(ofd, buf, left < sizeof buf ? left : sizeof buf);
            if (n <= 0 || write_all(fd, buf, (size_t)n) < 0)
                break;
            left -= (size_t)n;
        }
    }
    if (ofd >= 0)
        close(ofd);
}

static void serve_client(int fd, reader_t *r, char *line)
{
    for (;;) {
        char *args = strchr(line, ' ');
        if (args)
            *args++ = '\0';
        else
            args = "";

        if (strcmp(line, "SUBMIT") == 0)
            handle_submit(fd, args);
        else if (strcmp(line, "STATUS") == 0)
            handle_status(fd, args);
        else if (strcmp(line, "CANCEL") == 0)
            handle_cancel(fd, args);
        else if (strcmp(line, "FETCH") == 0)
            handle_fetch(fd, args);
        else
            send_line(fd, "ERROR unknown request");

        int n = read_line(r, line, MSG_MAX + 1);
        if (n == -2)
            send_line(fd, "ERROR line too long");
        if (n < 0)
            return;           /* client finished, or died half way through a line */
    }
}

/* ------------------------------------------------------------------ */
/* Workers                                                            */
/* ------------------------------------------------------------------ */

/* Output goes to its own file, on disk, before the job is journalled as finished. */
static int save_output(int num, const char *out, size_t len)
{
    char path[64];
    out_path(num, path, sizeof path);
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0)
        return -1;
    int rc = write_all(fd, out, len) == 0 && fsync(fd) == 0 ? 0 : -1;
    close(fd);
    return rc;
}

/* "DONE <id> <EXIT|TIMEOUT|CANCELLED|SIGNAL> <code> <len>" + len bytes of output */
static int handle_report(worker_t *w, reader_t *r, char *args)
{
    char id[64], kind[16];
    long code;
    size_t len;
    if (sscanf(args, "%63s %15s %ld %zu", id, kind, &code, &len) != 4 || len > OUTPUT_MAX) {
        logmsg("worker %d: bad report \"%s\"", w->id, args);
        return -1;
    }
    char *out = malloc(len + 1);
    if (!out || read_exact(r, out, len) < 0) {
        free(out);
        return -1;
    }

    pthread_mutex_lock(&lock);
    job_t *j = job_find(id);
    if (!j || j->worker != w) {
        logmsg("worker %d: report for %s, which it does not hold", w->id, id);
    } else {
        j->worker = NULL;
        w->free++;
        if (save_output(j->num, out, len) < 0)
            logmsg("WARNING: cannot save the output of %s: %s", id, strerror(errno));
        if (j->state == ST_RUNNING) {
            if (strcmp(kind, "EXIT") == 0)
                job_set_state(j, ST_DONE, (int)code);
            else if (strcmp(kind, "TIMEOUT") == 0)
                job_set_state(j, ST_KILLED_TIMEOUT, 0);
            else if (strcmp(kind, "CANCELLED") == 0)
                job_set_state(j, ST_CANCELLED, 0);
            else
                job_set_state(j, ST_KILLED_SIGNAL, (int)code);
        }
        /* already CANCELLED here: the client was told so, it stays so */
        logmsg("job %s finished on worker %d: %s %ld, %zu bytes of output",
               id, w->id, kind, code, len);
        pthread_cond_broadcast(&reported);
        dispatch();
    }
    pthread_mutex_unlock(&lock);
    free(out);
    return 0;
}

static void serve_worker(int fd, reader_t *r, const char *addr, char *hello)
{
    long k;
    if (parse_int(hello + 6, 1, 1000, &k) < 0) {
        send_line(fd, "ERROR usage: HELLO <K>");
        return;
    }

    worker_t *w = calloc(1, sizeof *w);
    if (!w)
        return;
    w->fd = fd;
    w->slots = w->free = (int)k;
    w->alive = 1;

    /* We write to workers while holding the table lock. A worker that stops
     * reading must not freeze everybody: give up on it after 2 s. */
    struct timeval tv = { .tv_sec = 2, .tv_usec = 0 };
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);

    pthread_mutex_lock(&lock);
    w->id = next_worker_id++;
    w->next = workers;
    workers = w;
    logmsg("worker %d connected from %s, K=%ld", w->id, addr, k);
    dispatch();
    pthread_mutex_unlock(&lock);

    char line[MSG_MAX + 1];
    const char *why = "connection closed";
    for (;;) {
        if (read_line(r, line, sizeof line) < 0)
            break;
        if (strncmp(line, "DONE ", 5) == 0) {
            if (handle_report(w, r, line + 5) < 0) {
                why = "broken report";
                break;
            }
        } else if (strcmp(line, "BYE") == 0) {
            why = "said goodbye";
            break;
        } else {
            logmsg("worker %d: ignoring \"%s\"", w->id, line);
        }
    }

    /* The worker is gone. Jobs it still held were not reported: put
     * RUNNING ones back in the queue, so another worker runs them. */
    pthread_mutex_lock(&lock);
    logmsg("worker %d %s", w->id, why);
    for (worker_t **pp = &workers; *pp; pp = &(*pp)->next)
        if (*pp == w) {
            *pp = w->next;
            break;
        }
    for (int i = 0; i < njobs; i++) {
        job_t *j = jobs[i];
        if (j->worker != w)
            continue;
        j->worker = NULL;
        if (j->state == ST_RUNNING) {
            job_set_state(j, ST_QUEUED, 0);
            logmsg("T%d-%d back in the queue", TEAM, j->num);
        }
    }
    pthread_cond_broadcast(&reported);
    dispatch();
    pthread_mutex_unlock(&lock);
    free(w);
}

/* ------------------------------------------------------------------ */
/* Restart                                                            */
/* ------------------------------------------------------------------ */

/* Replays jobs.log into the table, then puts RUNNING jobs back in the queue. */
static int recover(void)
{
    mkdir(OUT_DIR, 0755);              /* EEXIST is fine */

    journal_fd = journal_open(JOURNAL_PATH);   /* also cuts a torn last line */
    if (journal_fd < 0) {
        fprintf(stderr, "server: cannot open %s: %s\n", JOURNAL_PATH, strerror(errno));
        return -1;
    }
    fcntl(journal_fd, F_SETFD, FD_CLOEXEC);

    int lines = 0, skipped = 0;
    char line[JOURNAL_LINE];
    int rfd = journal_read_open(JOURNAL_PATH);
    while (journal_next(rfd, line, sizeof line) == 1) {
        lines++;
        if (apply(line) < 0)
            skipped++;
    }
    journal_read_close(rfd);

    /* A RUNNING job's worker died with the old server: run it again from the start. */
    int requeued = 0;
    for (int i = 0; i < njobs; i++)
        if (jobs[i]->state == ST_RUNNING) {
            job_set_state(jobs[i], ST_QUEUED, 0);
            requeued++;
        }

    if (lines > 0)
        logmsg("recovered %d jobs from %s (%d lines, %d skipped): %d queued, "
               "of them %d were RUNNING; next id T%d-%d",
               njobs, JOURNAL_PATH, lines, skipped, nqueue, requeued, TEAM, njobs + 1);
    return 0;
}

/* ------------------------------------------------------------------ */
/* Connections and main                                               */
/* ------------------------------------------------------------------ */

typedef struct {
    int  fd;
    char addr[64];
} conn_t;

static void *conn_thread(void *arg)
{
    conn_t *c = arg;
    reader_t *r = malloc(sizeof *r);
    char *line = malloc(MSG_MAX + 1);

    if (r && line) {
        reader_init(r, c->fd);
        int n = read_line(r, line, MSG_MAX + 1);
        if (n == -2)
            send_line(c->fd, "ERROR line too long");
        else if (n >= 0 && strncmp(line, "HELLO ", 6) == 0)
            serve_worker(c->fd, r, c->addr, line);
        else if (n >= 0)
            serve_client(c->fd, r, line);
    }

    close(c->fd);
    free(line);
    free(r);
    free(c);
    return NULL;
}

static void on_signal(int sig)
{
    int saved = errno;
    char b = sig == SIGUSR1 ? 'U' : 'T';
    if (write(sig_pipe[1], &b, 1) < 0) { /* pipe full: a byte is already waiting */ }
    errno = saved;
}

static void dump_table(void)
{
    char text[64];
    pthread_mutex_lock(&lock);
    logmsg("job table, %d jobs, %d queued:", njobs, nqueue);
    for (int i = 0; i < njobs; i++) {
        format_status(jobs[i], text, sizeof text);
        logmsg("  %s  priority %d  timeout %d  %s",
               text, jobs[i]->priority, jobs[i]->timeout, jobs[i]->cmd);
    }
    pthread_mutex_unlock(&lock);
}

int main(int argc, char **argv)
{
    long port;
    if (argc != 2 || parse_int(argv[1], 1, 65535, &port) < 0) {
        fprintf(stderr, "usage: ./server <port>\n");
        return 1;
    }

    const char *pol = getenv("POLICY");
    if (pol && strcmp(pol, "fifo") == 0)
        policy = POL_FIFO;
    const char *ag = getenv("AGING");
    long agv;
    if (ag && parse_int(ag, 1, 3600, &agv) == 0)
        aging_sec = (int)agv;

    /* Self-pipe: the handler only writes one byte, the main loop does the work. */
    if (pipe2(sig_pipe, O_CLOEXEC | O_NONBLOCK) < 0) {
        perror("server: pipe");
        return 1;
    }
    signal(SIGPIPE, SIG_IGN);
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGUSR1, &sa, NULL);

    /* A previous server may still be closing this port: retry briefly. */
    int lfd = -1;
    for (int tries = 0; tries < 30; tries++) {
        lfd = tcp_listen((int)port);
        if (lfd >= 0 || errno != EADDRINUSE)
            break;
        usleep(100000);
    }
    if (lfd < 0) {
        fprintf(stderr, "server: cannot listen on port %ld: %s\n", port, strerror(errno));
        return 1;
    }

    printf("server: listening on port %ld\n", port);
    fflush(stdout);

    /* The table is rebuilt before the first connection is accepted. */
    if (recover() < 0)
        return 1;
    logmsg("policy %s, aging %d s per level, job ids T%d-<n>, journal %s",
           policy == POL_FIFO ? "fifo" : "priority", aging_sec, TEAM, JOURNAL_PATH);

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

    for (;;) {
        struct pollfd p[2] = { { lfd, POLLIN, 0 }, { sig_pipe[0], POLLIN, 0 } };
        if (poll(p, 2, -1) < 0) {
            if (errno == EINTR)
                continue;
            perror("server: poll");
            break;
        }

        if (p[1].revents & POLLIN) {
            char b[16];
            int stop = 0;
            ssize_t n = read(sig_pipe[0], b, sizeof b);
            for (ssize_t i = 0; i < n; i++) {
                if (b[i] == 'T')
                    stop = 1;
                else if (b[i] == 'U')
                    dump_table();
            }
            if (stop)
                break;
        }

        if (p[0].revents & POLLIN) {
            struct sockaddr_storage sa_in;
            socklen_t salen = sizeof sa_in;
            int c = accept4(lfd, (struct sockaddr *)&sa_in, &salen, SOCK_CLOEXEC);
            if (c < 0) {
                if (errno == EMFILE || errno == ENFILE)
                    usleep(100000);    /* out of descriptors: do not spin */
                continue;
            }

            conn_t *conn = calloc(1, sizeof *conn);
            if (!conn) {
                close(c);
                continue;
            }
            conn->fd = c;
            if (sa_in.ss_family == AF_INET) {
                struct sockaddr_in *a = (struct sockaddr_in *)&sa_in;
                char ip[INET_ADDRSTRLEN] = "?";
                inet_ntop(AF_INET, &a->sin_addr, ip, sizeof ip);
                snprintf(conn->addr, sizeof conn->addr, "%s:%d", ip, ntohs(a->sin_port));
            } else {
                snprintf(conn->addr, sizeof conn->addr, "?");
            }
            logmsg("connection from %s", conn->addr);

            pthread_t tid;
            if (pthread_create(&tid, &attr, conn_thread, conn) != 0) {
                logmsg("cannot start a thread for %s", conn->addr);
                close(c);
                free(conn);
            }
        }
    }

    logmsg("SIGTERM received, closing port %ld and exiting", port);
    close(lfd);
    fflush(stdout);
    /* Every change is already in jobs.log; the kernel closes the sockets. */
    _exit(0);
}

/* worker.c -- runs jobs for the server.
 *
 *   ./worker <server-host> <server-port> [K]
 *
 * Connects to the server, announces K ("HELLO <K>"), then runs up to K
 * jobs at once. One thread, one poll() loop watching:
 *   - the server socket   ("RUN ..." and "CANCEL ..." arrive here),
 *   - one pipe per job    (the job's stdout and stderr),
 *   - a self-pipe         (SIGCHLD and SIGTERM handlers write one byte),
 *   - the nearest deadline, as poll()'s timeout.
 *
 * Every job is started by run_job() and nothing else calls fork().
 * Each job is its own process group, so a kill reaches everything it started:
 * sh -c "sleep 100" is two processes, kill(-pgid, ...) gets both.
 * The worker remembers WHY it killed a job, because waitpid() cannot tell
 * a timeout from a cancel from a kill by somebody else.
 */
#define _GNU_SOURCE
#include "common.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

typedef enum { WHY_NONE, WHY_TIMEOUT, WHY_CANCEL, WHY_SHUTDOWN } why_t;

typedef struct {
    int       used;
    char      id[64];
    pid_t     pid;          /* also the job's process group id */
    int       outfd;        /* read end of the job's pipe, -1 once closed */
    char     *out;
    size_t    outlen, outcap;
    long long deadline;     /* now_ms() at which the timeout fires */
    why_t     why;          /* why WE killed it, if we did */
} slot_t;

static slot_t *slots;
static int     nslots;
static int     server_fd = -1;
static int     sig_pipe[2] = { -1, -1 };

static void logmsg(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
static void logmsg(const char *fmt, ...)
{
    va_list ap;
    fputs("worker: ", stdout);
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    putchar('\n');
    fflush(stdout);
}

static void on_signal(int sig)
{
    int saved = errno;
    char b = sig == SIGCHLD ? 'C' : 'T';
    if (write(sig_pipe[1], &b, 1) < 0) { /* full: a byte is already waiting */ }
    errno = saved;
}

/* ------------------------------------------------------------------ */
/* Starting a job: the only fork() in the program                     */
/* ------------------------------------------------------------------ */

static int run_job(slot_t *s, const char *id, int timeout, const char *cmd)
{
    int p[2];
    if (pipe2(p, O_CLOEXEC) < 0)
        return -1;

    pid_t pid = fork();
    if (pid < 0) {
        close(p[0]);
        close(p[1]);
        return -1;
    }

    if (pid == 0) {
        /* child: own process group, default signals, output into the pipe */
        setpgid(0, 0);
        signal(SIGPIPE, SIG_DFL);   /* "ignored" would survive exec */
        signal(SIGCHLD, SIG_DFL);
        signal(SIGTERM, SIG_DFL);
        signal(SIGINT, SIG_DFL);
        sigset_t none;
        sigemptyset(&none);
        sigprocmask(SIG_SETMASK, &none, NULL);

        int devnull = open("/dev/null", O_RDONLY);
        if (devnull >= 0) {
            dup2(devnull, STDIN_FILENO);
            if (devnull != STDIN_FILENO)
                close(devnull);
        }
        dup2(p[1], STDOUT_FILENO);
        dup2(p[1], STDERR_FILENO);
        /* p[0], p[1], the server socket and the self-pipe are all
         * O_CLOEXEC, so the job inherits none of them */

        execl("/bin/sh", "sh", "-c", cmd, (char *)NULL);
        _exit(127);
    }

    /* parent: set the group here too, so a kill right after fork already works */
    setpgid(pid, pid);
    close(p[1]);                       /* the closing rule: keep only our end */
    fcntl(p[0], F_SETFL, fcntl(p[0], F_GETFL) | O_NONBLOCK);

    memset(s, 0, sizeof *s);
    s->used = 1;
    snprintf(s->id, sizeof s->id, "%s", id);
    s->pid = pid;
    s->outfd = p[0];
    s->deadline = now_ms() + (long long)timeout * 1000;
    s->why = WHY_NONE;
    return 0;
}

static void kill_job(slot_t *s, why_t why)
{
    if (s->why == WHY_NONE)
        s->why = why;
    kill(-s->pid, SIGKILL);            /* the whole process group */
}

/* ------------------------------------------------------------------ */
/* Output and reports                                                 */
/* ------------------------------------------------------------------ */

/* Reads what the pipe has right now. Closes it on EOF. */
static void read_output(slot_t *s)
{
    char buf[8192];
    while (s->outfd >= 0) {
        ssize_t n = read(s->outfd, buf, sizeof buf);
        if (n < 0 && errno == EINTR)
            continue;
        if (n < 0)
            return;                    /* EAGAIN: nothing more for now */
        if (n == 0) {
            close(s->outfd);
            s->outfd = -1;
            return;
        }
        if (s->outlen + (size_t)n > OUTPUT_MAX)
            continue;                  /* keep draining, keep nothing more */
        if (s->outlen + (size_t)n > s->outcap) {
            size_t cap = s->outcap ? s->outcap : 4096;
            while (cap < s->outlen + (size_t)n)
                cap *= 2;
            char *p = realloc(s->out, cap);
            if (!p)
                continue;
            s->out = p;
            s->outcap = cap;
        }
        memcpy(s->out + s->outlen, buf, (size_t)n);
        s->outlen += (size_t)n;
    }
}

/* The job's process was reaped with this status: tell the server, free the slot. */
static void finish_job(slot_t *s, int status)
{
    /* whatever is still in the pipe was written before the exit */
    read_output(s);
    if (s->outfd >= 0) {               /* a background child still holds it open */
        close(s->outfd);
        s->outfd = -1;
    }
    kill(-s->pid, SIGKILL);            /* leftovers of the job's group, if any */

    const char *kind;
    int code;
    if (s->why == WHY_TIMEOUT) {
        kind = "TIMEOUT"; code = 0;
    } else if (s->why == WHY_CANCEL) {
        kind = "CANCELLED"; code = 0;
    } else if (s->why == WHY_SHUTDOWN && !WIFEXITED(status)) {
        /* (a job that exited on its own just before the kill keeps its real code) */
        kind = "SIGNAL"; code = SIGKILL;
    } else if (WIFEXITED(status)) {
        kind = "EXIT"; code = WEXITSTATUS(status);
    } else if (WIFSIGNALED(status)) {
        kind = "SIGNAL"; code = WTERMSIG(status);
    } else {
        kind = "SIGNAL"; code = SIGKILL;
    }

    logmsg("%s finished: %s %d, %zu bytes of output", s->id, kind, code, s->outlen);

    /* best effort: a dead server is never a reason to hang */
    if (server_fd >= 0) {
        if (send_line(server_fd, "DONE %s %s %d %zu", s->id, kind, code, s->outlen) < 0 ||
            write_all(server_fd, s->out ? s->out : "", s->outlen) < 0) {
            close(server_fd);
            server_fd = -1;
        }
    }

    free(s->out);
    memset(s, 0, sizeof *s);
    s->outfd = -1;
}

static slot_t *slot_by_pid(pid_t pid)
{
    for (int i = 0; i < nslots; i++)
        if (slots[i].used && slots[i].pid == pid)
            return &slots[i];
    return NULL;
}

static slot_t *slot_by_id(const char *id)
{
    for (int i = 0; i < nslots; i++)
        if (slots[i].used && strcmp(slots[i].id, id) == 0)
            return &slots[i];
    return NULL;
}

/* The SIGCHLD reaper: waitpid(-1, ..., WNOHANG) until nothing is left. */
static void reap_children(void)
{
    int status;
    pid_t pid;
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        slot_t *s = slot_by_pid(pid);
        if (s)
            finish_job(s, status);
    }
}

/* Kill every job, reap every child, report them, say goodbye, exit. */
static void shutdown_worker(const char *why, int code)
{
    logmsg("%s: killing running jobs", why);
    reap_children();                   /* jobs that already ended keep their real status */

    for (int i = 0; i < nslots; i++)
        if (slots[i].used)
            kill_job(&slots[i], WHY_SHUTDOWN);

    for (int i = 0; i < nslots; i++) {
        if (!slots[i].used)
            continue;
        int status = 0;
        while (waitpid(slots[i].pid, &status, 0) < 0 && errno == EINTR)
            ;
        finish_job(&slots[i], status);
    }

    if (server_fd >= 0) {
        send_line(server_fd, "BYE");
        close(server_fd);
    }
    logmsg("exiting");
    exit(code);
}

/* ------------------------------------------------------------------ */
/* Messages from the server                                           */
/* ------------------------------------------------------------------ */

static void handle_server_line(char *line)
{
    if (strncmp(line, "RUN ", 4) == 0) {
        /* RUN <id> <timeout> <escaped command> */
        char id[64];
        int timeout, used = 0;
        static char cmd[CMD_MAX + 1];
        if (sscanf(line + 4, "%63s %d %n", id, &timeout, &used) < 2 || used == 0 ||
            timeout < 1 || unescape_cmd(line + 4 + used, cmd, sizeof cmd) < 0) {
            logmsg("bad RUN line: %s", line);
            return;
        }

        slot_t *s = NULL;
        for (int i = 0; i < nslots && !s; i++)
            if (!slots[i].used)
                s = &slots[i];

        if (!s || run_job(s, id, timeout, cmd) < 0) {
            logmsg("cannot start %s (%s)", id, s ? strerror(errno) : "no free slot");
            if (server_fd >= 0)
                send_line(server_fd, "DONE %s EXIT 127 0", id);
            return;
        }
        logmsg("%s started, pid %d, timeout %d s: %s", id, (int)s->pid, timeout, cmd);
    } else if (strncmp(line, "CANCEL ", 7) == 0) {
        slot_t *s = slot_by_id(line + 7);
        if (s) {
            logmsg("%s cancel requested: killing process group %d", s->id, (int)s->pid);
            kill_job(s, WHY_CANCEL);
        }
        /* not found: it already finished, its report is on the way */
    } else {
        logmsg("ignoring \"%s\"", line);
    }
}

int main(int argc, char **argv)
{
    long k = 1;
    if (argc != 3 && argc != 4) {
        fprintf(stderr, "usage: ./worker <server-host> <server-port> [K]\n");
        return 1;
    }
    if (argc == 4 && parse_int(argv[3], 1, 1000, &k) < 0) {
        fprintf(stderr, "worker: K must be an integer >= 1\n");
        return 1;
    }

    nslots = (int)k;
    slots = calloc((size_t)nslots, sizeof *slots);
    if (!slots)
        return 1;
    for (int i = 0; i < nslots; i++)
        slots[i].outfd = -1;

    if (pipe2(sig_pipe, O_CLOEXEC | O_NONBLOCK) < 0) {
        perror("worker: pipe");
        return 1;
    }
    signal(SIGPIPE, SIG_IGN);
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = on_signal;
    sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGCHLD, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);

    server_fd = tcp_connect(argv[1], argv[2]);
    if (server_fd < 0) {
        fprintf(stderr, "worker: cannot connect to %s:%s\n", argv[1], argv[2]);
        return 3;
    }
    if (send_line(server_fd, "HELLO %ld", k) < 0) {
        fprintf(stderr, "worker: cannot talk to %s:%s\n", argv[1], argv[2]);
        return 3;
    }
    printf("worker: connected to %s:%s, K=%ld\n", argv[1], argv[2], k);
    fflush(stdout);

    reader_t r;
    reader_init(&r, server_fd);
    struct pollfd *pfd = calloc((size_t)nslots + 2, sizeof *pfd);
    if (!pfd)
        return 1;

    for (;;) {
        /* the nearest deadline decides how long poll() may sleep */
        long long now = now_ms(), wait = -1;
        for (int i = 0; i < nslots; i++)
            if (slots[i].used && slots[i].why == WHY_NONE) {
                long long left = slots[i].deadline - now;
                if (left < 0)
                    left = 0;
                if (wait < 0 || left < wait)
                    wait = left;
            }

        int n = 0;
        pfd[n++] = (struct pollfd){ server_fd, POLLIN, 0 };
        pfd[n++] = (struct pollfd){ sig_pipe[0], POLLIN, 0 };
        for (int i = 0; i < nslots; i++)
            pfd[n++] = (struct pollfd){ slots[i].used ? slots[i].outfd : -1, POLLIN, 0 };

        if (poll(pfd, (nfds_t)n, (int)(wait > 600000 ? 600000 : wait)) < 0 && errno != EINTR) {
            perror("worker: poll");
            shutdown_worker("poll failed", 1);
        }

        /* signals */
        if (pfd[1].revents & POLLIN) {
            char b[64];
            int term = 0;
            ssize_t got;
            while ((got = read(sig_pipe[0], b, sizeof b)) > 0)
                for (ssize_t i = 0; i < got; i++)
                    if (b[i] == 'T')
                        term = 1;
            if (term)
                shutdown_worker("SIGTERM received", 0);
        }

        /* job output (before reaping, so nothing is left in the pipes) */
        for (int i = 0; i < nslots; i++)
            if (slots[i].used && slots[i].outfd >= 0 &&
                (pfd[2 + i].revents & (POLLIN | POLLHUP | POLLERR)))
                read_output(&slots[i]);

        reap_children();

        /* timeouts */
        now = now_ms();
        for (int i = 0; i < nslots; i++)
            if (slots[i].used && slots[i].why == WHY_NONE && now >= slots[i].deadline) {
                logmsg("%s timed out: killing process group %d", slots[i].id, (int)slots[i].pid);
                kill_job(&slots[i], WHY_TIMEOUT);
            }

        /* the server */
        if (server_fd >= 0 && (pfd[0].revents & (POLLIN | POLLHUP | POLLERR))) {
            if (reader_fill(&r) <= 0) {
                close(server_fd);
                server_fd = -1;
                shutdown_worker("lost the server", 1);
            }
            char line[MSG_MAX + 1];
            int len;
            while ((len = reader_take_line(&r, line, sizeof line)) != -1) {
                if (len == -2) {
                    logmsg("line from server too long, dropping it");
                    r.len = 0;
                    break;
                }
                handle_server_line(line);
            }
        }
        if (server_fd < 0)
            shutdown_worker("lost the server", 1);
    }
}

/* common.h -- helpers shared by server, worker and client.
 *
 * Everything on the wire is one line per message, ending in '\n'.
 * Because one write() is not one read(), every program reads through
 * a reader_t that buffers bytes until a whole line has arrived.
 */
#ifndef COMMON_H
#define COMMON_H

#include <stddef.h>
#include <sys/types.h>

/* Team number: every job id starts with it (T12-1, T12-2, ...).
 * Change it here or build with: make clean && make TEAM=7 */
#ifndef TEAM
#define TEAM 12
#endif

#define MSG_MAX     8192                /* longest protocol line, '\n' excluded */
#define CMD_MAX     3800                /* longest escaped command: must fit in one journal line */
#define OUTPUT_MAX  (16 * 1024 * 1024)  /* captured output kept per job */

typedef struct {
    int    fd;
    size_t len;              /* bytes waiting in buf */
    char   buf[MSG_MAX + 1];
} reader_t;

void reader_init(reader_t *r, int fd);

/* One read() into the buffer. Returns bytes read, 0 on EOF, -1 on error. */
ssize_t reader_fill(reader_t *r);

/* Takes one complete line out of the buffer (without '\n').
 * Returns its length, -1 if no complete line is buffered yet,
 * -2 if the buffer is full and still holds no '\n' (line too long). */
int reader_take_line(reader_t *r, char *out, size_t cap);

/* Blocking: reads until one whole line arrived.
 * Returns its length, -1 on EOF or error, -2 if the line is too long. */
int read_line(reader_t *r, char *out, size_t cap);

/* Blocking: exactly n bytes (buffered ones first). 0 on success, -1 on EOF/error. */
int read_exact(reader_t *r, void *out, size_t n);

/* Writes all n bytes, retrying short writes. 0 on success, -1 on error. */
int write_all(int fd, const void *buf, size_t n);

/* printf-style; appends '\n' and sends the line with one write_all. */
int send_line(int fd, const char *fmt, ...) __attribute__((format(printf, 2, 3)));

/* TCP helpers. Both return a socket fd or -1 (errno set). */
int tcp_listen(int port);
int tcp_connect(const char *host, const char *port);

/* Commands travel inside one line, so '\n', '\r' and '\\' are escaped:
 * "\\" stands for a backslash, "\n" for a newline, "\r" for a carriage
 * return. Return 0, or -1 if the result does not fit in cap. */
int escape_cmd(const char *in, char *out, size_t cap);
int unescape_cmd(const char *in, char *out, size_t cap);

/* Parses a whole decimal integer in [lo, hi]. 0 on success, -1 otherwise. */
int parse_int(const char *s, long lo, long hi, long *out);

/* Milliseconds from a monotonic clock. */
long long now_ms(void);

#endif

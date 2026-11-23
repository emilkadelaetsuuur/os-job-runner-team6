#define _GNU_SOURCE
#include "common.h"

#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

void reader_init(reader_t *r, int fd)
{
    r->fd = fd;
    r->len = 0;
}

ssize_t reader_fill(reader_t *r)
{
    if (r->len >= MSG_MAX)
        return -1;                       /* full: caller should take a line */
    for (;;) {
        ssize_t n = read(r->fd, r->buf + r->len, MSG_MAX - r->len);
        if (n < 0 && errno == EINTR)
            continue;
        if (n > 0)
            r->len += (size_t)n;
        return n;
    }
}

int reader_take_line(reader_t *r, char *out, size_t cap)
{
    char *nl = memchr(r->buf, '\n', r->len);
    if (nl == NULL)
        return r->len >= MSG_MAX ? -2 : -1;

    size_t linelen = (size_t)(nl - r->buf);
    size_t copy = linelen < cap - 1 ? linelen : cap - 1;
    memcpy(out, r->buf, copy);
    out[copy] = '\0';
    if (copy > 0 && out[copy - 1] == '\r')   /* tolerate CRLF from nc/telnet */
        out[--copy] = '\0';

    size_t rest = r->len - linelen - 1;
    memmove(r->buf, nl + 1, rest);
    r->len = rest;
    return (int)copy;
}

int read_line(reader_t *r, char *out, size_t cap)
{
    for (;;) {
        int n = reader_take_line(r, out, cap);
        if (n != -1)
            return n;                    /* a line, or -2 (too long) */
        if (reader_fill(r) <= 0)
            return -1;                   /* EOF in the middle of a line: drop it */
    }
}

int read_exact(reader_t *r, void *out, size_t n)
{
    char *p = out;
    size_t from_buf = r->len < n ? r->len : n;

    memcpy(p, r->buf, from_buf);
    memmove(r->buf, r->buf + from_buf, r->len - from_buf);
    r->len -= from_buf;
    p += from_buf;
    n -= from_buf;

    while (n > 0) {
        ssize_t k = read(r->fd, p, n);
        if (k < 0 && errno == EINTR)
            continue;
        if (k <= 0)
            return -1;
        p += k;
        n -= (size_t)k;
    }
    return 0;
}

int write_all(int fd, const void *buf, size_t n)
{
    const char *p = buf;
    while (n > 0) {
        ssize_t k = write(fd, p, n);
        if (k < 0 && errno == EINTR)
            continue;
        if (k < 0)
            return -1;
        p += k;
        n -= (size_t)k;
    }
    return 0;
}

int send_line(int fd, const char *fmt, ...)
{
    char line[MSG_MAX + 2];
    va_list ap;

    va_start(ap, fmt);
    int n = vsnprintf(line, MSG_MAX + 1, fmt, ap);
    va_end(ap);
    if (n < 0 || n > MSG_MAX)
        return -1;
    line[n++] = '\n';
    return write_all(fd, line, (size_t)n);
}

int tcp_listen(int port)
{
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0)
        return -1;

    /* Lets a new server bind the port at once, even while old
     * connections of the previous server sit in TIME_WAIT. */
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);

    struct sockaddr_in a;
    memset(&a, 0, sizeof a);
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_ANY);
    a.sin_port = htons((unsigned short)port);

    if (bind(fd, (struct sockaddr *)&a, sizeof a) < 0 || listen(fd, 64) < 0) {
        int e = errno;
        close(fd);
        errno = e;
        return -1;
    }
    return fd;
}

int tcp_connect(const char *host, const char *port)
{
    struct addrinfo hints, *res, *ai;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    if (getaddrinfo(host, port, &hints, &res) != 0)
        return -1;

    int fd = -1;
    for (ai = res; ai != NULL; ai = ai->ai_next) {
        fd = socket(ai->ai_family, ai->ai_socktype | SOCK_CLOEXEC, ai->ai_protocol);
        if (fd < 0)
            continue;
        if (connect(fd, ai->ai_addr, ai->ai_addrlen) == 0)
            break;
        close(fd);
        fd = -1;
    }
    freeaddrinfo(res);
    return fd;
}

int escape_cmd(const char *in, char *out, size_t cap)
{
    size_t j = 0;
    for (; *in; in++) {
        if (j + 3 > cap)
            return -1;
        if (*in == '\\') {
            out[j++] = '\\';
            out[j++] = '\\';
        } else if (*in == '\n') {
            out[j++] = '\\';
            out[j++] = 'n';
        } else if (*in == '\r') {          /* read_line strips a trailing '\r' */
            out[j++] = '\\';
            out[j++] = 'r';
        } else {
            out[j++] = *in;
        }
    }
    out[j] = '\0';
    return 0;
}

int unescape_cmd(const char *in, char *out, size_t cap)
{
    size_t j = 0;
    for (; *in; in++) {
        if (j + 2 > cap)
            return -1;
        if (in[0] == '\\' && in[1] == 'n') {
            out[j++] = '\n';
            in++;
        } else if (in[0] == '\\' && in[1] == 'r') {
            out[j++] = '\r';
            in++;
        } else if (in[0] == '\\' && in[1] == '\\') {
            out[j++] = '\\';
            in++;
        } else {
            out[j++] = *in;
        }
    }
    out[j] = '\0';
    return 0;
}

int parse_int(const char *s, long lo, long hi, long *out)
{
    char *end;
    if (s == NULL || *s == '\0')
        return -1;
    errno = 0;
    long v = strtol(s, &end, 10);
    if (errno != 0 || *end != '\0' || v < lo || v > hi)
        return -1;
    *out = v;
    return 0;
}

long long now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

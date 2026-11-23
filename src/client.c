/* client.c -- one request, one answer, exit.
 *
 *   ./client <host> <port> submit "<command>" <timeout> [priority]
 *   ./client <host> <port> status <id>
 *   ./client <host> <port> cancel <id>
 *   ./client <host> <port> fetch  <id>
 *
 * Exit codes: 0 ok, 1 "no" answer (already finished / not finished / error),
 * 2 unknown id, 3 cannot connect.
 */
#define _GNU_SOURCE
#include "common.h"

#include <ctype.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

static void usage(void)
{
    fprintf(stderr,
            "usage: ./client <host> <port> submit \"<command>\" <timeout> [priority]\n"
            "       ./client <host> <port> status|cancel|fetch <id>\n");
    exit(1);
}

/* An id is one printable word: anything else can never be one the server issued. */
static int is_word(const char *s)
{
    if (*s == '\0')
        return 0;
    for (; *s; s++)
        if (!isgraph((unsigned char)*s))
            return 0;
    return 1;
}

int main(int argc, char **argv)
{
    if (argc < 5)
        usage();

    const char *host = argv[1], *port = argv[2], *verb = argv[3];
    char request[MSG_MAX + 1];

    if (strcmp(verb, "submit") == 0) {
        long timeout, prio = 0;
        char esc[CMD_MAX];
        if (argc != 6 && argc != 7)
            usage();
        if (parse_int(argv[5], 1, 1000000000L, &timeout) < 0) {
            fprintf(stderr, "client: timeout must be a whole number of seconds, at least 1\n");
            return 1;
        }
        if (argc == 7 && parse_int(argv[6], 0, 9, &prio) < 0) {
            fprintf(stderr, "client: priority must be an integer 0-9\n");
            return 1;
        }
        if (argv[4][0] == '\0') {
            fprintf(stderr, "client: empty command\n");
            return 1;
        }
        if (escape_cmd(argv[4], esc, sizeof esc) < 0) {
            fprintf(stderr, "client: command too long\n");
            return 1;
        }
        snprintf(request, sizeof request, "SUBMIT %ld %ld %s", timeout, prio, esc);
    } else if (strcmp(verb, "status") == 0 || strcmp(verb, "cancel") == 0 ||
               strcmp(verb, "fetch") == 0) {
        if (argc != 5)
            usage();
        if (!is_word(argv[4]) || strlen(argv[4]) > 100) {
            printf("%s UNKNOWN\n", argv[4]);
            return 2;
        }
        snprintf(request, sizeof request, "%s %s",
                 verb[0] == 's' ? "STATUS" : verb[0] == 'c' ? "CANCEL" : "FETCH", argv[4]);
    } else {
        usage();
    }

    signal(SIGPIPE, SIG_IGN);

    int fd = tcp_connect(host, port);
    if (fd < 0) {
        fprintf(stderr, "client: cannot connect to %s:%s\n", host, port);
        return 3;
    }

    /* Never hang forever on a dead server. */
    struct timeval tv = { .tv_sec = 30, .tv_usec = 0 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);

    if (send_line(fd, "%s", request) < 0) {
        fprintf(stderr, "client: lost connection to %s:%s\n", host, port);
        return 1;
    }

    reader_t r;
    char reply[MSG_MAX + 1];
    reader_init(&r, fd);
    if (read_line(&r, reply, sizeof reply) < 0) {
        fprintf(stderr, "client: no answer from %s:%s\n", host, port);
        return 1;
    }

    /* reply = "<WORD> <rest>" */
    char *rest = strchr(reply, ' ');
    if (rest)
        *rest++ = '\0';
    else
        rest = "";

    int code = 1;
    if (strcmp(reply, "ID") == 0) {                 /* submit */
        printf("%s\n", rest);
        code = 0;
    } else if (strcmp(reply, "STATE") == 0) {       /* status */
        printf("%s\n", rest);
        code = 0;
    } else if (strcmp(reply, "CANCELLED") == 0) {   /* cancel */
        printf("%s CANCELLED\n", rest);
        code = 0;
    } else if (strcmp(reply, "FINISHED") == 0) {    /* cancel, too late */
        printf("%s ALREADY FINISHED\n", rest);
        code = 1;
    } else if (strcmp(reply, "OUTPUT") == 0) {      /* fetch: length-prefixed body */
        long len;
        if (parse_int(rest, 0, OUTPUT_MAX, &len) < 0) {
            fprintf(stderr, "client: bad answer from server\n");
        } else {
            char *body = malloc((size_t)len + 1);
            if (body == NULL || read_exact(&r, body, (size_t)len) < 0) {
                fprintf(stderr, "client: output cut short\n");
            } else {
                fwrite(body, 1, (size_t)len, stdout);
                code = 0;
            }
            free(body);
        }
    } else if (strcmp(reply, "NOTFINISHED") == 0) { /* fetch, too early */
        fprintf(stderr, "%s NOT FINISHED\n", rest);
        code = 1;
    } else if (strcmp(reply, "UNKNOWN") == 0) {
        printf("%s UNKNOWN\n", rest);
        code = 2;
    } else if (strcmp(reply, "ERROR") == 0) {
        fprintf(stderr, "client: server says: %s\n", rest);
    } else {
        fprintf(stderr, "client: bad answer from server\n");
    }

    fflush(stdout);
    close(fd);
    return code;
}

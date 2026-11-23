/* journal.h - a file that remembers what your server did.
 * SOC 3010 Operating Systems, Term Project, Details 2. Use it unchanged.
 *
 *   Writing (while the server runs):
 *       int j = journal_open("jobs.log");
 *       journal_append(j, "SUBMIT T12-7 10 0 echo hello");
 *
 *   Reading (once, when the server starts):
 *       int r = journal_read_open("jobs.log");
 *       char line[JOURNAL_LINE];
 *       while (journal_next(r, line, sizeof line) == 1)
 *           apply(line);                 // your function: change the job table
 *       journal_read_close(r);
 */
#ifndef JOURNAL_H
#define JOURNAL_H

#include <fcntl.h>
#include <string.h>
#include <unistd.h>

#define JOURNAL_LINE 4096   /* the longest line, with its '\n' */

/* Open the journal for writing. The file is created if it does not exist.
 * O_APPEND: every write goes to the end of the file, whoever writes it.
 * A torn last line (no '\n': the server died while writing it) is cut off
 * first, so the next line does not get glued onto it.
 * Returns a file descriptor, or -1. */
static inline int journal_open(const char *path)
{
    int fd = open(path, O_RDWR | O_CREAT | O_APPEND, 0644);
    off_t end;
    char c;

    if (fd < 0)
        return -1;
    end = lseek(fd, 0, SEEK_END);
    while (end > 0 && lseek(fd, end - 1, SEEK_SET) >= 0 &&
           read(fd, &c, 1) == 1 && c != '\n')
        end--;                           /* walk back to the last '\n' */
    if (ftruncate(fd, end) == -1) {
        close(fd);
        return -1;
    }
    return fd;
}

/* Add one line (give it without '\n') and wait until it is on the disk.
 * One write() for the whole line, so two writers never mix their lines.
 * Returns 0 when the line is safe, -1 if it was not written. */
static inline int journal_append(int fd, const char *line)
{
    char buf[JOURNAL_LINE];
    size_t n = strlen(line);

    if (n == 0 || n > JOURNAL_LINE - 1 || strchr(line, '\n') != NULL)
        return -1;                       /* empty, too long, or two lines */
    memcpy(buf, line, n);
    buf[n] = '\n';
    if (write(fd, buf, n + 1) != (ssize_t)(n + 1))
        return -1;
    if (fsync(fd) == -1)                 /* the OS writes it to the disk NOW */
        return -1;
    return 0;
}

/* Open the journal for reading. Returns -1 if there is no journal yet
 * (a first start: nothing to read, and that is not an error). */
static inline int journal_read_open(const char *path)
{
    return open(path, O_RDONLY);
}

/* Read the next complete line into line[] (without its '\n').
 * Returns 1 when it gave you a line, 0 at the end of the journal.
 * A last line with no '\n' is a line the server was writing when it died:
 * it is dropped, never returned. */
static inline int journal_next(int fd, char *line, int size)
{
    int n = 0;
    char c;

    if (fd < 0)
        return 0;
    while (read(fd, &c, 1) == 1) {
        if (c == '\n') {
            line[n] = '\0';
            return 1;
        }
        if (n < size - 1)
            line[n++] = c;               /* a longer line is cut, not lost */
    }
    return 0;                            /* end of file: a torn tail is ignored */
}

static inline void journal_read_close(int fd)
{
    if (fd >= 0)
        close(fd);
}

#endif

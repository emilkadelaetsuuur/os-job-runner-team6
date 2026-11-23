# Wire protocol

All connections are TCP to the server's single port. Every message is **one
line ending in `\n`**, read in a loop through a buffer, because one `write()`
is not one `read()`. The only exceptions are the two messages that carry job
output (`OUTPUT` and `DONE`): they announce a byte count and the raw bytes
follow, since output can have many lines and may not end in `\n`.

The first line on a connection decides what it is: `HELLO <K>` makes it a
worker, any other line makes it a client.

Commands are escaped so they always fit in one line: `\\` means a backslash,
`\n` a newline, `\r` a carriage return. The client escapes, the server stores
and forwards the escaped text (also in the journal), the worker unescapes
right before `exec`.

## Client → server

| Request                                | Reply                                              |
|----------------------------------------|----------------------------------------------------|
| `SUBMIT <timeout> <priority> <command>`| `ID T12-7`                                         |
| `STATUS <id>`                          | `STATE T12-7 QUEUED` / `RUNNING` / `DONE 0` / `KILLED TIMEOUT` / `KILLED SIGNAL 9` / `CANCELLED` |
| `CANCEL <id>`                          | `CANCELLED T12-7` or `FINISHED T12-7`              |
| `FETCH <id>`                           | `OUTPUT <n>` + exactly n bytes, or `NOTFINISHED T12-7` |
| any verb with an id never issued       | `UNKNOWN <id>`                                     |
| anything malformed                     | `ERROR <explanation>`                              |

`<timeout>` is whole seconds, at least 1. `<priority>` is 0–9, 9 highest, 0
the default. The client turns replies into the exact text and exit codes of
the contract (`ALREADY FINISHED` → exit 1, `NOT FINISHED` on stderr → exit 1,
`UNKNOWN` → exit 2, cannot connect → exit 3).

## Worker ↔ server

| Direction        | Message                                         | Meaning |
|------------------|-------------------------------------------------|---------|
| worker → server  | `HELLO <K>`                                     | first message: I run up to K jobs at once |
| server → worker  | `RUN <id> <timeout> <command>`                  | start this job (sent only when a slot is free) |
| server → worker  | `CANCEL <id>`                                   | kill this job's process group |
| worker → server  | `DONE <id> <kind> <code> <n>` + n bytes         | the job is gone, here is its output; kind is `EXIT` (code = exit status), `TIMEOUT`, `CANCELLED` or `SIGNAL` (code = signal number) |
| worker → server  | `BYE`                                           | clean goodbye (after SIGTERM, after all `DONE`s) |

A slot is freed only by `DONE`, never by `CANCEL`, so the server's count of
free slots always matches the processes really running on the worker.

## Journal (`jobs.log`, Part 2)

Two kinds of line, appended with `journal_append()` (one `write`, then `fsync`):

```
SUBMIT T12-7 <timeout> <priority> <escaped command>
STATE T12-7 <the status text: QUEUED | RUNNING | DONE 0 | KILLED TIMEOUT | KILLED SIGNAL 9 | CANCELLED>
```

Order of every change: journal line → table → answer. A job's output is
written to `out/T12-7` (and fsync'ed) before its final `STATE` line.

At start: open the journal (a torn last line is cut), replay every line with
`apply()` (only changes the table; unknown lines skipped), then every job
still RUNNING gets a new `STATE … QUEUED` line and waits for a worker. The
next id is one more than the last `SUBMIT`.

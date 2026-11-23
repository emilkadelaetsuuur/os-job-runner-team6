# Job runner — SOC 3010 Operating Systems term project

**Team number:** 12  <!-- TODO: your real team number, here AND in src/common.h (TEAM) -->

**Team leader:** _Name Surname_

| Component                         | Owner           |
|-----------------------------------|-----------------|
| Protocol and client               | _Name Surname_  |
| Scheduler and policy              | _Name Surname_  |
| Worker and limits                 | _Name Surname_  |
| Durability and recovery           | _Name Surname_  |
| Operations and report             | _Name Surname_  |
| Testing and load (teams of six)   | _Name Surname_  |

## Build

```
make                          # builds ./server ./worker ./client in the repository root
make clean
make clean && make TEAM=7     # job ids become T7-1, T7-2, ...
```

Needs only `build-essential` on Ubuntu 22.04 / 24.04. `src/journal.h` is the
file given with Details 2, unchanged.

## Run

```
./server <port>                          # e.g. ./server 5000
./worker <server-host> <server-port> [K] # e.g. ./worker 192.168.1.20 5000 2
./client <server-host> <server-port> submit "<command>" <timeout> [priority]
./client <server-host> <server-port> status <id>
./client <server-host> <server-port> cancel <id>
./client <server-host> <server-port> fetch <id>
```

The server keeps its journal in `jobs.log` and job outputs in `out/`, both in
the directory it was started from. Kill it with `kill -9` at any moment and
start it again: every job it ever issued is still there.

Scheduling policy is chosen with environment variables on the server:

```
./server 5000                      # priority with aging (default), 5 s per level
POLICY=fifo ./server 5000          # plain FIFO
AGING=2 ./server 5000              # priority with aging, 2 s per level
kill -USR1 <server pid>            # print the whole job table
```

## Layout

| Path                    | What                                                         |
|-------------------------|--------------------------------------------------------------|
| `src/common.[ch]`       | line reader, write_all, TCP helpers, command escaping        |
| `src/client.c`          | the client: one request, one answer, exit code               |
| `src/server.c`          | the server: threads per connection, job table, queue, policy, journal |
| `src/worker.c`          | the worker: fork/exec via `/bin/sh -c`, pipe, timeout, cancel|
| `src/journal.h`         | given with Details 2, unchanged                              |
| `docs/PROTOCOL.md`      | the wire format and the journal format                       |
| `tests/selfcheck.sh`    | rows 1–13, unattended                                        |
| `tests/load8.sh`        | eight clients at once                                        |
| `tests/policy_trace.sh` | the scheduling trace for the report                          |

## Design in six lines

- **Server**: one thread accepts; every connection gets its own thread, so a
  silent client blocks nobody. All jobs live in one table under one mutex;
  every state change goes through `job_set_state()`.
- **Journal**: `job_set_state()` and `handle_submit()` first `journal_append()`
  a line (fsync'ed), then change the table, then answer. At start `recover()`
  replays `jobs.log` with `apply()` (which never writes) and puts RUNNING jobs
  back to QUEUED. Output is saved in `out/<id>` before the final STATE line.
- **Policy**: priority 0–9 (9 highest) with aging: every `AGING` seconds in the
  queue adds one level (capped at 9); ties go to the older job.
- **Worker**: one `poll()` loop over the server socket, each job's pipe and a
  self-pipe for SIGCHLD/SIGTERM; the nearest deadline is the poll timeout.
  `run_job()` is the only `fork()`. Each job is its own process group, so
  timeout and cancel kill everything the job started.
- **Hygiene**: SIGCHLD reaper with `waitpid(-1, …, WNOHANG)`; every fd is
  `O_CLOEXEC`; pipe ends closed where not needed; SIGTERM via a self-pipe.
- **Worker leaves**: jobs it was still running go back to the queue.

## Testing

```
make && ./tests/selfcheck.sh       # rows 1–13 on 127.0.0.1:5000
./tests/policy_trace.sh            # dispatch order for the report
```

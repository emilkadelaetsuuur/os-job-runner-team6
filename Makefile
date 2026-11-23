# make        -> builds ./server ./worker ./client in the repository root
# make clean  -> removes what make built
# make clean && make TEAM=7 -> job ids become T7-1, T7-2, ...

CC     = gcc
TEAM  ?= 12
CFLAGS = -Wall -Wextra -O2 -pthread -DTEAM=$(TEAM)

all: server worker client

server: src/server.c src/common.c src/common.h src/journal.h
	$(CC) $(CFLAGS) -o server src/server.c src/common.c

worker: src/worker.c src/common.c src/common.h
	$(CC) $(CFLAGS) -o worker src/worker.c src/common.c

client: src/client.c src/common.c src/common.h
	$(CC) $(CFLAGS) -o client src/client.c src/common.c

test: all
	bash tests/selfcheck.sh

clean:
	rm -f server worker client

.PHONY: all test clean

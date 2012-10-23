CFLAGS=-Wall -g
LDFLAGS=-lzmq -v

all: main

main: murmur.o avl.o sketches.o

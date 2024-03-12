# Makrodefinitions
CC       = g++
CPPFLAGS = -Wall -Wextra -O2 -std=c++20
CFLAGS   =
LDFLAGS  =
LDLIBS   = -lboost_program_options -pthread

.PHONY: all clean

#Executable files
all: sikradio-sender sikradio-receiver

# Rules for linking
sikradio-sender: sikradio-sender.o
	$(CC) -o sikradio-sender $^ $(LDLIBS)
sikradio-receiver: sikradio-receiver.o
	$(CC) -o sikradio-receiver $^ $(LDLIBS)

# Rules for compiling
sikradio-sender: structures.h err.h
sikradio-receiver: structures.h err.h

# clean
clean:
	rm -f *.o sikradio-sender
	rm -f *.o sikradio-receiver
CC = g++
CCFLAGS = -Wall -g -std=c++03
OBJS = serializer.o

.PHONY: all clean

all: $(OBJS)

serializer.o: serializer.cc
	$(CC) -c -o $@ $(CCFLAGS) $^

clean:
	rm -rf $(OBJS)

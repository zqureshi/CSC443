CC = g++
OPTS = -Wall -std=c++03

all: library.o create_random_file get_histogram

library.o: library.cc library.h
	$(CC) $(OPTS) -o $@ -c library.cc

create_random_file: create_random_file.cc library.o
	$(CC) $(OPTS) -o $@ $^

get_histogram: get_histogram.cc library.o
	$(CC) $(OPTS) -o $@ $^

clean:
	rm -f *.o create_random_file get_histogram

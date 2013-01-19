CC = gcc
CPPFLAGS = -Wall
TARGETS = create_random_file get_histogram

.PHONY: all clean

all: $(TARGETS)

clean:
	rm -f $(TARGETS) *.o

create_random_file: create_random_file.o
	$(CC) $(CPPFLAGS) -o $@ $^

get_histogram: get_histogram.o
	$(CC) $(CPPFLAGS) -o $@ $^

%.o: %.c
	$(CC) $(CPPFLAGS) -c -o $@ $<

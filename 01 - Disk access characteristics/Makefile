CC = gcc
TARGETS = create_random_file

all: $(TARGETS)

create_random_file: create_random_file.o library.o
	$(CC) -o $@ $^

%.o: %.c
	$(CC) -c -o $@ $<

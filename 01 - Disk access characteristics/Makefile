CC = gcc
CPPFLAGS = -Wall
TARGETS = create_random_file

all: $(TARGETS)

clean:
	rm -f $(TARGETS) *.o

create_random_file: create_random_file.o
	$(CC) -o $@ $^

%.o: %.c
	$(CC) $(CPPFLAGS) -c -o $@ $<

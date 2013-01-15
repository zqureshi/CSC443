CC = gcc

create_random_file: create_random_file.o
	$(CC) -o $@ $<

%.o: %.c
	$(CC) -c -o $@ $<

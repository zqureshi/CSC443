#include <stdio.h>
#include <stdlib.h>
#include <sys/timeb.h>

/**
 * Returns the current system time in milliseconds.
 */
inline long now() {
    struct timeb t;
    ftime(&t);
    return t.time * 1000 + t.millitm;
}

/**
 * Fills the given buffer with the given number of random characters between
 * 'A' and 'Z'.
 */
void random_array(char *array, long num) {
    while (num--)
        *(array++) = (rand() % 26) + 'A';
}



int main(int argc, char *argv[])
{
    // Initialize random seed
    srand(now() / 1000);

    if (argc != 4) {
        printf("USAGE: %s <filename> <total bytes> <block size>\n", argv[0]);
        return 1;
    }

    // Read arguments.
    long num_bytes  = atol(argv[2]),
         block_size = atol(argv[3]);
    if (num_bytes <= 0) {
        printf("Invalid number of total bytes (%s).\n", argv[2]);
        return 1;
    }
    if (block_size <= 0) {
        printf("Invalid block size (%s).\n", argv[3]);
        return 1;
    }

    // Open the file
    char *filename = argv[1];
    FILE *file     = fopen(filename, "w");
    if (!file) {
        printf("Could not open %s.\n", filename);
        return 1;
    }


    char *buffer = calloc(1, block_size);
    long start_time = now();

    // Write to disk
    while (num_bytes >= block_size) {
        random_array(buffer, block_size);
        fwrite(buffer, 1, block_size, file);
        fflush(file);

        num_bytes -= block_size;
    }

    // If any number of bytes is left, write that too.
    if (num_bytes) {
        random_array(buffer, num_bytes);
        fwrite(buffer, 1, num_bytes, file);
        fflush(file);
    }

    long end_time = now();
    printf("%ld %ld\n", block_size, end_time - start_time);
    fclose(file);
    return 0;
}


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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
 * Measures character frequencies in the file.
 */
int get_histogram(FILE *file, long hist[], int block_size, long *total_time,
        long *total_bytes)
{
    // Clear values
    *total_bytes = 0;
    *total_time  = 0;
    memset(hist, 0, sizeof(long) * 26);

    char *buffer    = calloc(1, block_size);
    long start_time = now();

    // Read blocks from file.
    size_t bytes_read = 0;
    while ((bytes_read = fread(buffer, 1, block_size, file))) {
        int i;
        *total_bytes += bytes_read;

        // Update histogram with results from this block
        for (i = 0; i < bytes_read; i++)
            hist[*(buffer + i) - 'A']++;

        // Clear buffer
        memset(buffer, 0, block_size);
    }
    long end_time = now();

    // Return -1 if there was an error.
    if (ferror(file))
        return (*total_time = -1);
    else
        return (*total_time = end_time - start_time);
}

int main(int argc, char *argv[])
{
    // Initialize random seed
    srand(now() / 1000);

    if (argc != 3) {
        printf("USAGE: %s <filename> <block size>\n", argv[0]);
        return 1;
    }

    // Read arguments.
    long block_size = atol(argv[2]);
    if (block_size <= 0) {
        printf("Invalid block size (%s).\n", argv[2]);
        return 1;
    }

    // Open the file
    char *filename = argv[1];
    FILE *file     = fopen(filename, "r");
    if (!file) {
        printf("Could not open %s.\n", filename);
        return 1;
    }

    long hist[26];
    long total_time  = 0;
    long total_bytes = 0;

    get_histogram(file, hist, block_size, &total_time, &total_bytes);

    int i;
    for (i = 0; i < 26; i++)
        fprintf(stderr, "%c %ld\n", i + 'A', hist[i]);
    fprintf(stderr, "Total bytes: %ld\n", total_bytes);

    if (total_time == -1)
        perror("An error occurred while trying to read the file.\n");
    else
        printf("%ld %ld\n", block_size, total_time);

    fclose(file);
    return 0;
}

#include <time.h>
#include <stdio.h>
#include <stdlib.h>

#include "library.h"

/**
 * populate array with num_bytes bytes of random data.
 */
void random_array(char *array, long num_bytes);

struct crf_ctx {
    FILE *file;
    long num_bytes;
    long block_size;
};

void create_random_file(void *context_)
{
    struct crf_ctx *context = (struct crf_ctx *) context_;

    long num_bytes  = context->num_bytes,
         block_size = context->block_size;

    char buffer[block_size];
    while (num_bytes > block_size)
    {
        random_array(buffer, block_size);
        num_bytes -= block_size;
        fwrite(buffer, 1, block_size, context->file);
        fflush(context->file);
    }

    random_array(buffer, num_bytes);
    fwrite(buffer, 1, num_bytes, context->file);
    fflush(context->file);
}

int main(int argc, char *argv[])
{
    // Initialize random seed
    srand(time(NULL));

    if (argc != 4)
    {
        printf("USAGE: %s <filename> <total bytes> <block size>\n", argv[0]);
        return 1;
    }

    char *filename    = argv[1];
    long num_bytes  = atol(argv[2]),
         block_size = atol(argv[3]);

    if (num_bytes <= 0 || block_size <= 0)
    {
        printf("Invalid block size or total number of bytes.\n");
        return 1;
    }

    struct crf_ctx context;
    context.num_bytes = num_bytes;
    context.block_size = block_size;
    context.file = fopen(filename, "w");

    long run_time = with_timer(&create_random_file, &context);
    printf("%ld %ld\n", block_size, run_time);
    return 0;
}

void random_array(char *array, long num_bytes)
{
    int i;
    for (i = 0; i < num_bytes; i++) {
        *(array + i) = (rand() % 26) + 'A';
    }
}

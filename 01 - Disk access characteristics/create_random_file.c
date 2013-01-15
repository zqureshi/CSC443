#include <stdio.h>

/**
 * populate array with num_bytes bytes of random data.
 */
void random_array(char *array, long num_bytes);

/**
 * parse the given string of numbers into a long.
 */
long parse_long(char *s);

int main(int argc, char *argv[])
{
    if (argc != 4)
    {
        printf("USAGE: %s <filename> <total bytes> <blocksize>\n", argv[0]);
        return 1;
    }

    char *filename    = argv[1],
         *num_bytes_  = argv[2],
         *block_size_ = argv[3];

    long num_bytes  = parse_long(num_bytes_),
         block_size = parse_long(block_size_);

    char buffer[block_size];
    while (num_bytes > block_size)
    {
        random_array(buffer, block_size);
        num_bytes -= block_size;
        // TODO write to file
    }

    random_array(buffer, num_bytes);
    // TODO write to file

    return 0;
}

void random_array(char *array, long num_bytes)
{
    // TODO [...]
}

long parse_long(char *s)
{
    // TODO [...]
    return 0;
}

#include <cstdio>
#include <cstdlib>
#include <sys/timeb.h>
#include "library.h"

inline long min(int a, int b) {
  return a < b ? a : b;
}

int main(int argc, char *argv[]) {
  if(argc != 4) {
    printf("Usage: create_random_file <filename> <total bytes> <block size>\n");
    return 1;
  }

  FILE *file = fopen(argv[1], "w");
  long total_bytes = atol(argv[2]);
  long block_size = atol(argv[3]);

  char buffer[block_size];

  while(total_bytes > 0) {
    random_array(buffer, block_size);
    fwrite(buffer, 1, min(block_size, total_bytes), file);
    fflush(file);

    total_bytes -= block_size;
  }

  fclose(file);
  return 0;
}

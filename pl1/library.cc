#include <cstdlib>

/**
 * Populate random array (which is already allocated with enough memory
 * to hold n bytes).
 */
void random_array(char *array, long bytes) {
  for(long i = 0; i < bytes; i++) {
    array[i] = 'A' + rand() % 26;
  }
}

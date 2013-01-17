#ifndef LIBRARY_H
#define LIBRARY_H

/**
 * Return the number of milliseconds it takes to call the given function with
 * the given context.
 */
long with_timer(void (*f)(void *), void *context);

/**
 * Return the current time in milliseconds.
 */
long now();

/**
 * Populare array with num bytes.
 *
 * Assumes there is enough space in the array.
 */
void random_array(char *array, long num);

#endif /* end of include guard: LIBRARY_H */

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

#endif /* end of include guard: LIBRARY_H */

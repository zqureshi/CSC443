#include "library.h"

#include <sys/timeb.h>

/**
 * Return the number of milliseconds it takes to call the given function.
 */
long with_timer(void (*f)(void *), void *context)
{
    long start = now();
    (*f)(context);
    long end = now();
    return end - start;
}


long now()
{
    struct timeb t;
    ftime(&t);
    return t.time * 1000 + t.millitm;
}

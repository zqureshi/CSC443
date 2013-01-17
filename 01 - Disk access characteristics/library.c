#include "library.h"

#include <stdlib.h>
#include <sys/timeb.h>

long with_timer(void (*f)(void*), void *context)
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

void random_array(char *array, long num)
{
    while (num-- > 0)
        *(array++) = (rand() % 26) + 'A';
}

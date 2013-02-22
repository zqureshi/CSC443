#ifndef LIBRARY_H
#define LIBRARY_H

#include <sys/timeb.h>

class Record; // Forward declaration.

/**
 * Return the current time in milliseconds.
 */
inline long now() {
    struct timeb t;
    ftime(&t);
    return t.time * 1000 + t.millitm;
}

void printrecord(Record *record);

#endif /* end of include guard: LIBRARY_H */


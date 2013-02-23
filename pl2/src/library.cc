#include <cstdio>
#include "library.h"
#include "serializer.h"

void printrecord(FILE *csv, Record *record) {
    bool first = true;
    for (Record::iterator it = record->begin(); it != record->end(); it++) {
        const char *attr = *it;
        fprintf(csv, (first ? "%s" : ",%s"), attr);
        first = false;
    }
    fprintf(csv, "\n");
}



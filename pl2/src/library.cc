#include <cstdio>
#include "library.h"
#include "serializer.h"

void printrecord(FILE *csv, Record *record) {
    bool first;
    for (Record::iterator it = record->begin(); it != record->end(); it++)
        if (first) {
            first = false;
            fprintf(csv, "%s", *it);
        } else
            fprintf(csv, ",%s", *it);
    printf("\n");
}



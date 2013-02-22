#include "library.h"
#include "serializer.h"

void printrecord(Record *record) {
    bool first;
    for (Record::iterator it = record->begin(); it != record->end(); it++)
        if (first) {
            first = false;
            printf("%s\n", *it);
        } else
            printf(",%s\n", *it);
    printf("\n");
}



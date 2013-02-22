#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "library.h"
#include "serializer.h"
#include "pagemanager.h"
#include "heapmanager.h"

#define RECORD_SIZE 1000

int main(int argc, char **argv)
{
    if (argc < 4) {
        printf("USAGE: %s <heap file> <start> <end> [page size]\n", argv[0]);
        exit(1);
    }

    char *heapfile = *(argv++),
         *start    = *(argv++),
         *end      = *(argv++);
    int page_size  = argc > 4 ? atoi(*argv) : 16384; // Default to 16K

    FILE *heapf = fopen(heapfile, "r");
    if (!heapf) {
        printf("Error opening file.\n");
        exit(1);
    }

    int record_count = 0;
    long start_time = now();

    Heapfile heap;
    init_heapfile(&heap, page_size, heapf);

    RecordIterator recordIter(&heap);
    while (recordIter.hasNext()) {
        Record record = recordIter.next();
        const char *a1 = record.at(0);
        const char *a2 = record.at(1);

        // TODO
        // For each SUBSTRING(a2, 1, 5), get number of records that match that
        // substring.

        record_count++;
    }

    long end_time = now();
    printf("NUMBER OF RECORDS: %d\n", record_count);
    printf("TIME: %ld milliseconds\n", end_time - start_time);

    fclose(heapf);
    return 0;
}

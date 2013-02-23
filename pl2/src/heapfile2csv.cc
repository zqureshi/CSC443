#include <stdio.h>
#include <stdlib.h>

#include "library.h"
#include "serializer.h"
#include "pagemanager.h"
#include "heapmanager.h"

#define RECORD_SIZE 1000

int main(int argc, char **argv)
{
    if (argc < 3) {
        printf("USAGE: %s <heap file> <csv file> [page size]\n", argv[0]);
        exit(1);
    }

    char *heapfile = *(argv++),
         *csvfile  = *(argv++);
    int page_size  = argc > 3 ? atoi(*argv) : 16384; // Default to 16K

    FILE *heapf = fopen(heapfile, "r"),
         *csvf  = fopen( csvfile, "w");
    if (!csvf || !heapf) {
        printf("Error opening file(s).\n");
        exit(1);
    }

    int record_count = 0;
    long start_time = now();

    Heapfile heap;
    init_heapfile(&heap, page_size, heapf);

    RecordIterator recordIter(&heap);
    while (recordIter.hasNext()) {
        Record record = recordIter.next();
        printrecord(&record);
        record_count++;
    }

    long end_time = now();
    printf("NUMBER OF RECORDS: %d\n", record_count);
    printf("TIME: %ld milliseconds\n", end_time - start_time);

    fclose(heapf);
    fclose(csvf);
    return 0;
}

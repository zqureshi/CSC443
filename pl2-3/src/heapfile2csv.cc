#include <stdio.h>
#include <stdlib.h>

#include "library.h"
#include "serializer.h"
#include "pagemanager.h"
#include "heapmanager.h"

#define RECORD_SIZE 1000
extern Schema csvSchema;

int main(int argc, char **argv)
{
    if (argc < 3) {
        printf("USAGE: %s <heap file> <csv file> [page size]\n", argv[0]);
        exit(1);
    }

    char *heapfile = argv[1],
         *csvfile  = argv[2];
    int page_size  = argc > 3 ? atoi(argv[3]) : 16384; // Default to 16K

    FILE *heapf = fopen(heapfile, "r"),
         *csvf  = csvfile[0] == '-' ? stdout : fopen( csvfile, "w");
    if (!csvf || !heapf) {
        printf("Error opening file(s).\n");
        exit(1);
    }

    int record_count = 0;
    long start_time = now();

    Heapfile heap;
    init_heapfile(&heap, page_size, heapf, false);

    RecordIterator recordIter(&heap, csvSchema);
    while (recordIter.hasNext()) {
        Record record = recordIter.next();
        printrecord(csvf, &record);
        record_count++;
    }

    long end_time = now();
    printf("NUMBER OF RECORDS: %d\n", record_count);
    printf("TIME: %ld milliseconds\n", end_time - start_time);

    fclose(heapf);
    if (csvf != stdout)
        fclose(csvf);
    return 0;
}

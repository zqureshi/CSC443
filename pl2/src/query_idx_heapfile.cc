#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <string>
#include <map>

#include "library.h"
#include "serializer.h"
#include "pagemanager.h"
#include "heapmanager.h"

Schema schema(2, 10);

int main(int argc, char **argv)
{
    if (argc < 4) {
        printf("USAGE: %s <heap file> <start> <end> [page size]\n", argv[0]);
        exit(1);
    }

    char *heapfile = argv[1],
         *start    = argv[2],
         *end      = argv[3];
    int page_size  = argc > 4 ? atoi(argv[4]) : 16384; // Default to 16K

    FILE *heapf = fopen(heapfile, "r");
    if (!heapf) {
        printf("Error opening file.\n");
        exit(1);
    }

    long start_time = now();

    Heapfile heap;
    init_heapfile(&heap, page_size, heapf, false);

    std::map<std::string, int> counts;
    RecordIterator recordIter(&heap, schema);
    while (recordIter.hasNext()) {
        Record record = recordIter.next();
        const char *a1 = record.at(0);

        // If not (A1 >= start and A2 <= end), skip
        if (strcmp(a1, start) < 0 || strcmp(a1, end) > 0) {
            continue;
        }

        // Get SUBSTRING(A2, 1, 5)
        char *a2 = (char*)record.at(1); a2[5] = 0;
        std::string suba2(a2);

        counts[suba2]++;
    }

    long end_time = now();

    std::map<std::string, int>::iterator it;
    for (it = counts.begin(); it != counts.end(); ++it)
        printf("%s %d\n", it->first.c_str(), it->second);

    printf("TIME: %ld milliseconds\n", end_time - start_time);

    fclose(heapf);
    return 0;
}

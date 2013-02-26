#include <stdio.h>
#include <stdlib.h>

#include "library.h"
#include "serializer.h"
#include "pagemanager.h"

// Number of bytes taken by a record.
#define RECORD_SIZE 1000

int main(int argc, char **argv) {
    if (argc != 3) {
        printf("%s <page file> <page size>\n", argv[0]);
        exit(1);
    }

    size_t page_size = atoi(argv[2]);

    FILE *csvf = stdout;
    FILE *pagef = fopen(argv[1], "r");

    if (!csvf || !pagef) {
        printf("Error opening file(s).\n");
        exit(1);
    }

    long start_time = now();
    int page_count = 0;
    int record_count = 0;

    Page page;
    init_fixed_len_page(&page, page_size, RECORD_SIZE);

    int capacity = fixed_len_page_capacity(&page);

    fread(page.data, 1, page.page_size, pagef);
    while (!feof(pagef)) {
        page_count++;

        // Print records from the page
        Record r;
        for (int i = 0; i < capacity; ++i) {
            if (read_fixed_len_page(&page, i, &r)) {
                printrecord(stdout, &r);
                record_count++;
            }
        }

        fread(page.data, 1, page.page_size, pagef);
    }

    long end_time = now();
    printf("NUMBER OF RECORDS: %d\n", record_count);
    printf("NUMBER OF PAGES: %d\n", page_count);
    printf("TIME: %ld milliseconds\n", end_time - start_time);

    fclose(pagef);
    return 0;
}

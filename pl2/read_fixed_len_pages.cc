#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

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

    Page page;
    init_fixed_len_page(&page, page_size, RECORD_SIZE);

    int capacity = fixed_len_page_capacity(&page);
    while (!feof(pagef)) {
        // Read page
        if (fread(page.data, 1, page.page_size, pagef) != RECORD_SIZE &&
                ferror(pagef)) {
            printf("Error reading from file.\n");
            goto CLEAN;
        }

        // Print records from the page
        Record r;
        for (int i = 0; i < capacity; ++i) {
            read_fixed_len_page(&page, i, &r);
            bool first = true;
            for (Record::iterator it = r.begin(); it != r.end(); it++) {
                printf(first ? "%s" : ",%s", *it);
                first = false;
            }
            printf("\n");
        }
    }

CLEAN:
    fclose(pagef);
    return 0;
}

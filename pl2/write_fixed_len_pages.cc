#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include "serializer.h"
#include "pagemanager.h"

// Number of bytes taken by a record.
#define RECORD_SIZE 1000

int main(int argc, char **argv) {
    if (argc != 4) {
        printf("%s <csv file> <page file> <page size>\n", argv[0]);
        exit(1);
    }

    size_t page_size = atoi(argv[3]);

    FILE *csvf = fopen(argv[1], "r");
    FILE *pagef = fopen(argv[2], "w");
    if (!csvf || !pagef) {
        printf("Error opening file(s).\n");
        exit(1);
    }

    Page page;
    init_fixed_len_page(&page, page_size, RECORD_SIZE);

    int capacity = fixed_len_page_capacity(&page);
    int added = 0;
    while (!feof(csvf)) {
        if (added == capacity) {
            // Write page to file and re-initialize.
            fwrite(page.data, 1, page.page_size, pagef);
            init_fixed_len_page(&page, page_size, RECORD_SIZE);
            added = 0;
        }

        char buf[RECORD_SIZE];
        Record r;

        if (fread((void*)buf, 1, RECORD_SIZE, csvf) != RECORD_SIZE) {
            if (ferror(csvf)) {
                printf("Error reading from file.\n");
                goto CLEAN;
            }
        }

        // Fill the record with the CSV data
        int pos = 0;
        char *start;
        char *end;
        for (start = end = buf; end < buf + RECORD_SIZE; end++) {
            if (*end == ',') {
                memcpy((char*)r.at(pos++), start, end - start);
                start = end + 1; // +1 to skip the comma
            }
        }

        int slot = add_fixed_len_page(&page, &r);
        added++;
        assert(slot != -1);
    }

CLEAN:
    fclose(pagef);
    fclose(csvf);
    return 0;
}

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/timeb.h>

#include "serializer.h"
#include "pagemanager.h"

// Number of bytes taken by a record.
#define RECORD_SIZE 1000
#define COLUMN_COUNT 100

// Buffer needs to have enough space for the columns, the 9 commas, the
// newline, and a \0.
#define BUFFER_SIZE (RECORD_SIZE + COLUMN_COUNT + 1)

/**
 * Returns the current system time in milliseconds.
 */
inline long now() {
    struct timeb t;
    ftime(&t);
    return t.time * 1000 + t.millitm;
}

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

    long start_time = now();

    Page page;
    init_fixed_len_page(&page, page_size, RECORD_SIZE);

    int total_records = 0;
    int page_count = 0;

    int capacity = fixed_len_page_capacity(&page);
    int added = 0; // number of records added to the current page
    while (!feof(csvf)) {
        if (added == capacity) {
            // Write page to file and re-initialize.
            fwrite(page.data, 1, page.page_size, pagef);
            init_fixed_len_page(&page, page_size, RECORD_SIZE);
            page_count++;
            total_records += added;
            added = 0;
        }

        char buf[BUFFER_SIZE];
        Record r;

        fgets(buf, BUFFER_SIZE, csvf);
        if (ferror(csvf)) {
            printf("Error reading from file.\n");
            fclose(csvf);
            fclose(pagef);
            exit(1);
        }

        // Fill the record with the CSV data
        int pos = 0;
        char *tok = strtok(buf, ",");
        while (tok != NULL) {
            strcpy((char*)r.at(pos++), tok);
            tok = strtok(NULL, ",");
        }

        int slot = add_fixed_len_page(&page, &r);
        added++;
        assert(slot != -1);
    }

    // Unfilled pgae
    if (added > 0) {
        fwrite(page.data, 1, (1 + RECORD_SIZE) * added, pagef);
        init_fixed_len_page(&page, page_size, RECORD_SIZE);
        page_count++;
        total_records += added;
    }

    long end_time = now();
    printf("NUMBER OF RECORDS: %d\n", total_records);
    printf("NUMBER OF PAGES: %d\n", page_count);
    printf("TIME: %ld milliseconds\n", end_time - start_time);

    fclose(pagef);
    fclose(csvf);
    return 0;
}

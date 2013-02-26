#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sstream>
#include <fstream>
#include <string>

#include "library.h"
#include "serializer.h"
#include "pagemanager.h"

// Number of bytes taken by a record.
#define RECORD_SIZE 1000
#define COLUMN_COUNT 100

// Buffer needs to have enough space for the columns, the 9 commas, the
// newline, and a \0.
#define BUFFER_SIZE (RECORD_SIZE + COLUMN_COUNT + 1)

int main(int argc, char **argv) {
    if (argc != 4) {
        printf("%s <csv file> <page file> <page size>\n", argv[0]);
        exit(1);
    }

    size_t page_size = atoi(argv[3]);

    std::ifstream csvf(argv[1]);
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
    while (csvf) {
        Record r;

        std::string buf;
        buf.reserve(RECORD_SIZE);

        if (!std::getline(csvf, buf))
            break;

        // Fill the record with the CSV data
        std::istringstream stream(buf);

        // Not using strtok anymore. Major bug:
        //      strtok is repeating the first column of the last row after
        //      returning the last column of the last row. I have no idea why.
        //
        // Yeah... that was fun to track down.
        int pos = 0;
        while (stream) {
            char attr[11];
            if (!stream.getline(attr, 11, ',')) break;
            strcpy((char*)r.at(pos++), attr);
        }

        int slot = add_fixed_len_page(&page, &r);
        added++;
        assert(slot != -1);

        if (added == capacity) {
            // Write page to file and re-initialize.
            fwrite(page.data, 1, page.page_size, pagef);
            init_fixed_len_page(&page, page_size, RECORD_SIZE);
            page_count++;
            total_records += added;
            added = 0;
        }
    }

    // Unfilled pgae
    if (added > 0) {
        fwrite(page.data, 1, (1 + RECORD_SIZE) * added, pagef);
        page_count++;
        total_records += added;
    }

    long end_time = now();
    printf("NUMBER OF RECORDS: %d\n", total_records);
    printf("NUMBER OF PAGES: %d\n", page_count);
    printf("TIME: %ld milliseconds\n", end_time - start_time);

    fclose(pagef);
    csvf.close();
    return 0;
}

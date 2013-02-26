#include <stdlib.h>
#include <stdio.h>
#include <gtest/gtest.h>
#include "heapmanager.h"

#define PAGE_SIZE 4 * 1024
#define RECORD_SIZE (csvSchema.numAttrs * csvSchema.attrLen)

/* Import internal functions from heapmanager */
extern Page *_init_page(Heapfile *heapfile, const Schema& schema);
extern void _free_page(Page *page);
extern int _locate_pageId(Heapfile *heapfile, PageID pageId, Page **dirPage, int *dirSlot);

/* Record population function from tests/serializer */
void populate_full_record(Record *recordIn) {
    for(int i = 0; i < csvSchema.numAttrs; i++) {
        sprintf((char *) recordIn->at(i), "%d", i);
    }
}

TEST(InitHeap, InitHeap) {
    FILE *file = fopen("empty.test.heap", "w+");
    Heapfile heap;

    /* Open heap for writing which creates directory record */
    init_heapfile(&heap, PAGE_SIZE, file, true);
    fclose(file);

    ASSERT_EQ(PAGE_SIZE, heap.page_size);
    ASSERT_EQ(file, heap.file_ptr);

    /* Now try reading heap file to validate directory record */
    file = fopen("empty.test.heap", "r+");
    init_heapfile(&heap, PAGE_SIZE, file, false);

    ASSERT_EQ(PAGE_SIZE, heap.page_size);
    ASSERT_EQ(file, heap.file_ptr);

    fclose(file);
}

TEST(HeapManager, AllocPage) {
    FILE *file = fopen("alloc.test.heap", "w+");
    Heapfile heap;

    /* Open heap for writing which creates directory record */
    init_heapfile(&heap, PAGE_SIZE, file, true);

    /* Allocate pages to fill primary directory entry */
    for(int i = 0; i < 314; i++)
        ASSERT_EQ(i, alloc_page(&heap));
    // NOTE: This test should always be the first test to call alloc_page
    // since it depends on a static variable starting at 0.

    /* Make sure we can insert more pages than can fit in one directory page */
    for(int i = 314; i < (315 + 900); i++)
        ASSERT_EQ(i, alloc_page(&heap));

    fclose(file);
}

TEST(HeapManager, RWPage) {
    FILE *file = fopen("rwpage.test.heap", "w+");
    Heapfile heap;

    /* Open heap for writing which creates directory record */
    init_heapfile(&heap, PAGE_SIZE, file, true);

    /* Allocate page */
    PageID pageId = alloc_page(&heap);

    /* Check PageID offset */
    Page *directory;
    int slot;

    ASSERT_EQ(PAGE_SIZE, _locate_pageId(&heap, pageId, &directory, &slot));

    /* Read back page */
    Page *page = _init_page(&heap, csvSchema);
    ASSERT_TRUE(read_page(&heap, pageId, page));

    /* Write data to page */
    Record record(csvSchema);
    populate_full_record(&record);
    int record_slot = add_fixed_len_page(page, &record, csvSchema);
    ASSERT_EQ(0, record_slot);

    /* Store page capacity and free slots */
    int capacity = fixed_len_page_capacity(page);
    int free_slots = fixed_len_page_freeslots(page);

    /* Write page to heapfile */
    ASSERT_TRUE(write_page(&heap, pageId, page));

    /* Read back page again and compare records */
    Page* readPage = _init_page(&heap, csvSchema);
    ASSERT_TRUE(read_page(&heap, pageId, readPage));

    /* Check capacity and slots */
    ASSERT_EQ(capacity, fixed_len_page_capacity(readPage));
    ASSERT_DOUBLE_EQ(free_slots, fixed_len_page_freeslots(readPage));

    /* Compare records */
    Record readRecord(csvSchema);
    read_fixed_len_page(readPage, record_slot, &readRecord, csvSchema);

    for(int i = 0; i < csvSchema.numAttrs; i++)
        ASSERT_STREQ(record.at(i), readRecord.at(i));

    /* Free all pages and records */
    _free_page(directory);
    _free_page(page);
    _free_page(readPage);

    fclose(file);
}

TEST(HeapManager, RWPageMany) {
    FILE *file = tmpfile();

    Heapfile heap;
    init_heapfile(&heap, PAGE_SIZE, file, true);

    /**
     * Fill the heap directory making sure we insert more pages than
     * can fit in one directory page.
     */
    for (int i = 0; i < 900; ++i) {
        // Allocate page in heapfile
        PageID pid = alloc_page(&heap);

        // Allocate page in memory and read from heapfile
        Page page;
        init_fixed_len_page(&page, heap.page_size, RECORD_SIZE);
        ASSERT_TRUE(read_page(&heap, pid, &page));

        // Fill page in-memory and write to heap file
        int num_records = 1 + (rand() % fixed_len_page_capacity(&page));
        int record_slots[num_records];

        for (int j = 0; j < num_records; ++j) {
            Record record(csvSchema);
            populate_full_record(&record);
            record_slots[j] = add_fixed_len_page(&page, &record, csvSchema);
        }

        ASSERT_TRUE(write_page(&heap, pid, &page));

        // Read back page and confirm that shit is the same
        Page readPage;
        init_fixed_len_page(&readPage, heap.page_size, RECORD_SIZE);
        ASSERT_TRUE(read_page(&heap, pid, &readPage));

        // Confirm that the number of filled slots is the same as the number
        // of records we added.
        ASSERT_EQ(num_records, fixed_len_page_capacity(&page) -
                               fixed_len_page_freeslots(&page));

        for (int j = 0; j < num_records; ++j) {
            Record record(csvSchema);
            populate_full_record(&record);

            Record readRecord(csvSchema);
            ASSERT_TRUE(read_fixed_len_page(&page, record_slots[j], &readRecord));

            for(int i = 0; i < csvSchema.numAttrs; i++)
                ASSERT_STREQ(record.at(i), readRecord.at(i));
        }

        delete [] (char*) page.data;
        delete [] (char*) readPage.data;
    }

    fclose(file);
}


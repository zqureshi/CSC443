#include <cstdio>
#include <cstdlib>
#include <gtest/gtest.h>
#include "serializer.h"
#include "pagemanager.h"
#include "constants.h"

TEST(PageCapacity, ZeroPageSize) {
    Page p = {NULL, 0, 10};
    ASSERT_EQ(0, fixed_len_page_capacity(&p)) << "Capacity of Page of Size 0 should be 0!";
}

TEST(PageCapacity, SlotEqualsPageSize) {
    Page p = {NULL, 100, 100};
    ASSERT_EQ(0, fixed_len_page_capacity(&p)) << "Won't have any space for index if slot_size == page_size!";
}

TEST(PageCapacity, PageSize100SlotSize10) {
    Page p = {NULL, 100, 10};
    ASSERT_EQ(9, fixed_len_page_capacity(&p)) << "9 * 10 (slots) + 9 (slots_directory) = 99";
}

TEST(PageCapacity, PageSize100SlotSize9) {
    Page p = {NULL, 100, 9};
    ASSERT_EQ(10, fixed_len_page_capacity(&p)) << "10 * 9 (slots) + 10 (slots_directory) = 100";
}

TEST(PageInit, PageInitialization) {
    int page_size = 100, slot_size = 10;
    Page p;

    init_fixed_len_page(&p, page_size, slot_size);
    ASSERT_EQ(page_size, p.page_size);
    ASSERT_EQ(slot_size, p.slot_size);
    ASSERT_NE((void *) NULL, p.data);

    delete [] (char *) p.data;
}

TEST(PageSlots, EmptyPage) {
    int page_size = 0, slot_size = 10;
    Page p;
    init_fixed_len_page(&p, page_size, slot_size);
    ASSERT_EQ(0, fixed_len_page_freeslots(&p)) << "Empty page should have 0 free slots!";

    delete [] (char *) p.data;
}

TEST(PageSlots, NewPage) {
    int page_size = 100, slot_size = 10;
    Page p;
    init_fixed_len_page(&p, page_size, slot_size);
    ASSERT_EQ(fixed_len_page_capacity(&p), fixed_len_page_freeslots(&p)) << "Newly created page should have all slots free!";

    delete [] (char *) p.data;
}

TEST(PageRecords, RecordManagement) {
    int page_size = 2002, slot_size = 1000;
    Page p;
    init_fixed_len_page(&p, page_size, slot_size);

    /* Populate record */
    Record r1;
    for(int i = 0; i < csvSchema.numAttrs; i++)
        sprintf((char *) r1.at(i), "1%8d", i);
    int slot1 = add_fixed_len_page(&p, &r1);
    ASSERT_EQ(0, slot1);

    Record r2;
    for(int i = 0; i < csvSchema.numAttrs; i++)
        sprintf((char *) r2.at(i), "2%8d", i);
    int slot2 = add_fixed_len_page(&p, &r2);
    ASSERT_EQ(1, slot2);

    ASSERT_EQ(-1, add_fixed_len_page(&p, &r2));

    Record r1_read;
    read_fixed_len_page(&p, slot1, &r1_read);
    for (int i = 0; i < csvSchema.numAttrs; ++i)
        ASSERT_STREQ(r1.at(i), r1_read.at(i));

    Record r2_read;
    read_fixed_len_page(&p, slot2, &r2_read);
    for (int i = 0; i < csvSchema.numAttrs; ++i)
        ASSERT_STREQ(r2.at(i), r2_read.at(i));

    delete [] (char *) p.data;
}

TEST(PageRecordIterator, FillAndIterate) {
    Schema schema(10, 10);
    int page_size = 1024;

    Page page;
    init_fixed_len_page(&page, page_size, fixed_len_sizeof(NULL, schema));

    int capacity     = fixed_len_page_capacity(&page);
    int record_count = capacity;

    // Fill the page with record_count records.
    for (int rec_num = 0; rec_num < record_count; ++rec_num) {
        Record record(schema);
        for (int attr_num = 0; attr_num < schema.numAttrs; ++attr_num) {
            sprintf((char*) record.at(attr_num), "%d-%d", rec_num, attr_num);
        }

        ASSERT_EQ(rec_num, add_fixed_len_page(&page, &record, schema));
    }

    int rec_num = 0;
    PageRecordIterator iterator(&page, schema);

    // Confirm that each record matches.
    while (iterator.hasNext()) {
        Record record = iterator.next();
        for (int attr_num = 0; attr_num < schema.numAttrs; ++attr_num) {
            char buf[10];
            sprintf(buf, "%d-%d", rec_num, attr_num);
            ASSERT_STREQ(buf, (char*)record.at(attr_num));
        }
        rec_num++;
    }
    ASSERT_EQ(record_count, rec_num);

    delete [] (char*) page.data;
}

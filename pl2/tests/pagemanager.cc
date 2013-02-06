#include "pagemanager.h"
#include <gtest/gtest.h>

using namespace std;

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
}

TEST(PageSlots, EmptyPage) {
    int page_size = 0, slot_size = 10;
    Page p;
    init_fixed_len_page(&p, page_size, slot_size);
    ASSERT_EQ(0, fixed_len_page_freeslots(&p)) << "Empty page should have 0 free slots!";
}

TEST(PageSlots, NewPage) {
    int page_size = 100, slot_size = 10;
    Page p;
    init_fixed_len_page(&p, page_size, slot_size);
    ASSERT_EQ(fixed_len_page_capacity(&p), fixed_len_page_freeslots(&p)) << "Newly created page should have all slots free!";
}

int main(int argc, char *argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

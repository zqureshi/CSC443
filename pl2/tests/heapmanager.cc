#include <gtest/gtest.h>
#include "heapmanager.h"

TEST(InitHeap, InitHeap) {
    int page_size = 4 * 1024;
    FILE *file = fopen("empty.test.heap", "w+");
    Heapfile heap;

    /* Open heap for writing which creates directory record */
    init_heapfile(&heap, page_size, file, true);
    fclose(file);

    ASSERT_EQ(page_size, heap.page_size);
    ASSERT_EQ(file, heap.file_ptr);

    /* Now try reading heap file to validate directory record */
    file = fopen("empty.test.heap", "r+");
    init_heapfile(&heap, page_size, file, false);

    ASSERT_EQ(page_size, heap.page_size);
    ASSERT_EQ(file, heap.file_ptr);
}

TEST(HeapManager, AllocPage) {
    int page_size = 4 * 1024;
    FILE *file = fopen("alloc.test.heap", "w+");
    Heapfile heap;

    /* Open heap for writing which creates directory record */
    init_heapfile(&heap, page_size, file, true);

    /* Allocate pages to fill primary directory entry */
    for(int i = 0; i < 314; i++)
        ASSERT_EQ(i, alloc_page(&heap));

    /* Make sure we can't insert any more pages */
    for(int i = 0; i < 315; i++)
        ASSERT_EQ(-1, alloc_page(&heap));

    fclose(file);
}

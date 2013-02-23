#ifndef HEAPMANAGER_H_
#define HEAPMANAGER_H_

#include <cstdio>
#include "pagemanager.h"

typedef struct {
    FILE *file_ptr;
    int page_size;
} Heapfile;

typedef int PageID;

typedef struct {
    int page_offset;
    int slot;
} RecordID;

extern Schema heapSchema;

/**
 * Initalize a heapfile to use the file and page size given.
 */
void init_heapfile(Heapfile *heapfile, int page_size, FILE *file, bool newHeap = false);

/**
 * Allocate another page in the heapfile.  This grows the file by a page.
 */
PageID alloc_page(Heapfile *heapfile, const Schema& schema = csvSchema);

/**
 * Read a page into memory
 */
bool read_page(Heapfile *heapfile, PageID pid, Page *page);

/**
 * Write a page from memory to disk
 */
bool write_page(Heapfile *heapfile, PageID pid, Page *page);

/**
 * Iterates through all the records of a page.
 */
class PageIterator {
    public:
        PageIterator(Page *page, const Schema &schema = csvSchema);
        bool hasNext();
        Record peek();
        Record next();
    private:
        const Schema &schema_;
        Record record_;
        Page *page_;
        int slot_;
        int capacity_;
};

/**
 * Iterates through all the directories in a heap.
 */
class HeapDirectoryIterator {
    public:
        HeapDirectoryIterator(Heapfile *heap);
        ~HeapDirectoryIterator();
        bool hasNext();
        Page *next();
    private:
        // The heap whose directories are being iterated.
        Heapfile *heap_;

        // Last read directory page.
        Page *directory_;

        // Offset of the next page to be read, or -1 if end reached.
        int dir_offset_;
};

class RecordIterator {
    public:
        RecordIterator(Heapfile *heap, const Schema &schema = csvSchema);
        ~RecordIterator();
        Record next();
        bool hasNext();
    private:
        const Schema &schema_;
        Heapfile *heap_;

        // Current directory
        Page *directory_;

        // Current page
        Page *page_;

        // Used to iterate through records of the current directory.
        PageIterator *directory_iter_;

        // Used to iterate through records of the current page.
        PageIterator *page_iter_;
};

#endif /* HEAPMANAGER_H_ */

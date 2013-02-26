#ifndef HEAPMANAGER_H_
#define HEAPMANAGER_H_

#include <cstdio>
#include "pagemanager.h"

typedef int PageID;

typedef struct {
    FILE *file_ptr;
    int page_size;
    PageID nextId;
    int64_t last_dir_offset;
} Heapfile;

typedef struct {
    int page_offset;
    int slot;
} RecordID;

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
 * Iterates through all the directories in a heap.
 */
class HeapDirectoryIterator {
    public:
        HeapDirectoryIterator(Heapfile *heap);
        ~HeapDirectoryIterator();
        bool hasNext();
        Page *next();
        int64_t offset();
    private:
        // The heap whose directories are being iterated.
        Heapfile *heap_;

        // Last read directory page.
        Page *directory_;

        // Offset of the next page to be read, or -1 if end reached.
        int64_t dir_offset_;
};

/**
 * Iterates through all the pages specified in a heap directory.
 */
class DirectoryPageIterator {
    public:
        DirectoryPageIterator(Heapfile *heap, Page *directory,
                const Schema &schema = csvSchema);
        ~DirectoryPageIterator();
        bool hasNext();
        Page *next();
    private:
        const Schema &schema_;
        // Heap containing the pages.
        Heapfile *heap_;
        // Directory whose page records are being iterated through.
        Page *directory_;
        // Last read page.
        Page *page_;
        // Iterator used to iterate through page records in the directory.
        PageRecordIterator dir_iter_;
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

        // The combination of the following three iterators is used to check
        // all records of all pages of all directories in the heap.
        HeapDirectoryIterator   heap_iter_;
        DirectoryPageIterator *  dir_iter_;
        PageRecordIterator    * page_iter_;
};

#endif /* HEAPMANAGER_H_ */

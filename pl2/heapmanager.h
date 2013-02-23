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

class PageIterator {
    public:
        PageIterator(Page *page, const Schema &schema = csvSchema)
                : schema_(schema), record_(schema) {
            page_ = page;
            slot_ = 0;
            capacity_ = fixed_len_page_capacity(page);
        }

        bool hasNext() {
            if (slot_ >= capacity_) {
                return false; // Page exhausted
            }

            // Keep going until a non-empty slot is found.
            while (slot_ < capacity_ &&
                    !read_fixed_len_page(page_, slot_, &record_)) {
                slot_++;
            }

            return slot_ < capacity_;
        }

        Record peek() {
            return record_;
        }

        Record next() {
            // We assume that hasNext has already been called.
            assert(slot_ < capacity_);
            slot_++;
            return record_;
        }
    private:
        const Schema &schema_;
        Record record_;

        Page *page_;
        int slot_;
        int capacity_;
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

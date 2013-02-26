#include <cassert>
#include <cstring>
#include <cstdio>
#include <stdint.h>
#include "heapmanager.h"

typedef struct {
    int64_t offset;    /* Offset of current page */
    int64_t next;      /* Offset of next page */
    uint64_t sig;      /* Magic number */
} DirHdr;

typedef struct {
    int64_t pageId;
    int64_t offset;
    int64_t slots;
} DirRecord;

Schema heapSchema(3, 8);

inline PageID _get_new_id(Heapfile *heapfile) {
    return heapfile->nextId++;
}

Page *_init_page(Heapfile *heapfile, const Schema& schema = heapSchema) {
    Page *page = new Page;
    init_fixed_len_page(page, heapfile->page_size, schema.numAttrs * schema.attrLen);
    return page;
}

void _free_page(Page *page) {
    delete [] (char *) page->data;
    delete page;
}

/**
 * Locate PageID in directory and return its offset and set
 * directory and slot pointers, else return -1.
 *
 * TODO: Handle traversing overflow directory pages.
 */
int64_t _locate_pageId(Heapfile *heapfile, PageID pageId, Page **dirPage, int *dirSlot) {
    assert(dirPage != NULL);
    assert(dirSlot != NULL);

    FILE *file = heapfile->file_ptr;
    int page_size = heapfile->page_size;

    Page *directory = _init_page(heapfile);
    Record hdrRecord(heapSchema);
    DirHdr *dirHdr;

    int64_t offset = -1, slot;

    int64_t directory_offset, next = DIR_OFFSET;
    do {
        directory_offset = next;
        fseeko(file, directory_offset, SEEK_SET);
        fread(directory->data, page_size, 1, file);

        /**
         * Locate the PageID in directory and return offset. The first
         * record of a directory page is always a DirHdr so skip it.
         */
        for(slot = 1; slot < fixed_len_page_capacity(directory); slot++) {
            Record dirRecord(heapSchema);

            /* If slot is non-empty then check it */
            if(read_fixed_len_page(directory, slot, &dirRecord, heapSchema)) {
                DirRecord *pageRecord = (DirRecord *) dirRecord.at(0);
                if(pageRecord->pageId == pageId) {
                    offset = pageRecord->offset;
                    break;
                }
            }
        }

        /* Fetch the next pointer */
        assert(true == read_fixed_len_page(directory, 0, &hdrRecord, heapSchema));

        dirHdr = (DirHdr *) (hdrRecord.at(0));
        assert(dirHdr->offset == directory_offset);
        assert(dirHdr->sig == DIRHDR_SIG);

        /* Set next pointer */
        next = dirHdr->next;
    } while(next != DIRHDR_NULL);

    if(offset == -1) {
        /* Free directory page */
        _free_page(directory);

        return offset;
    }

    /* Set directory page and slot pointers */
    *dirPage = directory;
    *dirSlot = slot;

    return offset;
}

/**
 * Initialize a new directory page with proper values.
 */
void _init_dir_page(Page *directory, int64_t offset) {
    /* First directory entry of a heap page is a pointer to next or NULL */
    Record hdrRecord(heapSchema);
    DirHdr *dirHdr = (DirHdr *) (hdrRecord.at(0));
    dirHdr->offset = offset;
    dirHdr->next = DIRHDR_NULL;
    dirHdr->sig  = DIRHDR_SIG;

    /* Populate directory header and make sure it's at the first slot */
    assert(0 == add_fixed_len_page(directory, &hdrRecord, heapSchema));
}

/**
 * Initalize a heapfile to use the file and page size given.
 */
void init_heapfile(Heapfile *heapfile, int page_size, FILE *file, bool newHeap) {
    /* Set file pointer and page sizes */
    heapfile->file_ptr = file;
    heapfile->page_size = page_size;
    heapfile->last_dir_offset = DIR_OFFSET;

    /**
     * NOTE: Since we start PageID numbering at 0 even when opening a
     * new heapfile, this would cause problems if you open a heapfile for
     * reading and then try allocating new pages since they'll have the
     * same PageIDs as old ones.
     *
     * Ideally we'd store the latest PageID in the heapfile itself and read
     * it into memory when initializing a heapfile for reading.
     */
    heapfile->nextId = 0;

    /* Initialize primary directory page */
    Page *directory = _init_page(heapfile);

    /* If creating new heap then write primary directory page */
    if(newHeap) {
        _init_dir_page(directory, DIR_OFFSET);

        /* Write page to disk */
        fseeko(file, DIR_OFFSET, SEEK_SET);
        fwrite(directory->data, page_size, 1, file);
        fflush(file);
    } else {
        /* Otherwise validate that heap file has proper directory record */
        fseeko(file, DIR_OFFSET, SEEK_SET);
        fread(directory->data, page_size, 1, file);

        Record hdrRecord(heapSchema);
        assert(true == read_fixed_len_page(directory, 0, &hdrRecord, heapSchema));

        DirHdr *dirHdr = (DirHdr *) (hdrRecord.at(0));
        assert(dirHdr->offset == DIR_OFFSET);
        assert(dirHdr->sig == DIRHDR_SIG);
    }

    /* Release memory for pages */
    _free_page(directory);
}

/**
 * Allocate another page in the heapfile.  This grows the file by a page.
 *
 * TODO: Support overflows by linking directory pages.
 */
PageID alloc_page(Heapfile *heapfile, const Schema& schema) {
    FILE *file = heapfile->file_ptr;
    int page_size = heapfile->page_size;
    int slot;

    /* Allocate page */
    Page *page = _init_page(heapfile, schema);

    /* Create directory record */
    fseeko(file, 0, SEEK_END);
    int64_t offset = ftello(file);
    PageID pageId = _get_new_id(heapfile);
    Record dirRecord(heapSchema);
    DirRecord *pageRecord = (DirRecord *) dirRecord.at(0);

    pageRecord->pageId = pageId;
    pageRecord->offset = offset;
    pageRecord->slots = fixed_len_page_freeslots(page);

    /* Append page to file */
    fseeko(file, offset, SEEK_SET);
    fwrite(page->data, page_size, 1, file);
    fflush(file);

    /**
     * Since we don't support page deletion, a new page can only be added
     * to the last directory page so try to add, if it is full then we need
     * to create a new directory page and link it.
     */
    Page *directory = _init_page(heapfile);
    Record hdrRecord(heapSchema);
    DirHdr *dirHdr;

    /* Load directory page */
    int64_t directory_offset = heapfile->last_dir_offset;
    fseeko(file, directory_offset, SEEK_SET);
    fread(directory->data, page_size, 1, file);

    assert(true == read_fixed_len_page(directory, 0, &hdrRecord, heapSchema));

    dirHdr = (DirHdr *) (hdrRecord.at(0));
    assert(dirHdr->offset == directory_offset);
    assert(dirHdr->sig == DIRHDR_SIG);

    /**
     * At this point should be at the last directory page, try adding record
     */
    if((slot = add_fixed_len_page(directory, &dirRecord, heapSchema)) == -1) {
        /**
         * Last directory page was full so allocate new one and link it
         */
        fseeko(file, 0, SEEK_END);
        int64_t new_directory_offset = ftello(file);

        /* Update header in current directory page and write it */
        heapfile->last_dir_offset = new_directory_offset;
        dirHdr->next = new_directory_offset;
        write_fixed_len_page(directory, 0, &hdrRecord, heapSchema);

        fseeko(file, directory_offset, SEEK_SET);
        fwrite(directory->data, page_size, 1, file);
        fflush(file);

        /* Set current offset to new offset */
        directory_offset = new_directory_offset;

        /* Free current directory page and re-initiate it */
        _free_page(directory);
        directory = _init_page(heapfile, heapSchema);
        _init_dir_page(directory, directory_offset);

        /* Add page record to newly directory page, this should never fail */
        assert((slot = add_fixed_len_page(directory, &dirRecord, heapSchema)) != -1);
    }

    /* Write directory to disk */
    fseeko(file, directory_offset, SEEK_SET);
    fwrite(directory->data, page_size, 1, file);
    fflush(file);

    /* Free pages */
    _free_page(page);
    _free_page(directory);

    return pageId;
}

/**
 * Read Page with given PageID from Heapfile else return false.
 */
bool read_page(Heapfile *heapfile, PageID pid, Page *page) {
    Page *directory;
    int64_t offset;
    int slot;

    /* If record for given PageID doesn't exist then fail */
    if((offset = _locate_pageId(heapfile, pid, &directory, &slot)) == -1) {
        return false;
    }

    /* Read page from disk */
    fseeko(heapfile->file_ptr, offset, SEEK_SET);
    fread(page->data, heapfile->page_size, 1, heapfile->file_ptr);

    /* Free up directory page */
    _free_page(directory);

    return true;
}

/**
 * Write page with given PageID to HeapFile else return false.
 */
bool write_page(Heapfile *heapfile, PageID pid, Page *page) {
    FILE *file = heapfile->file_ptr;
    int page_size = heapfile->page_size;

    Page *directory;
    int64_t offset;
    int slot;

    /* If record for given PageID doesn't exist then fail */
    if((offset = _locate_pageId(heapfile, pid, &directory, &slot)) == -1) {
        return false;
    }

    /* Update free slot information in page record */
    Record dirRecord(heapSchema);
    assert(true == read_fixed_len_page(directory, slot, &dirRecord, heapSchema));

    DirRecord *pageRecord = (DirRecord *) dirRecord.at(0);
    pageRecord->slots = fixed_len_page_freeslots(page);

    write_fixed_len_page(directory, slot, &dirRecord, heapSchema);

    /* Get directory offset from its header and write it to disk */
    assert(true == read_fixed_len_page(directory, 0, &dirRecord, heapSchema));
    DirHdr *dirHdr = (DirHdr *) dirRecord.at(0);

    fseeko(file, dirHdr->offset, SEEK_SET);
    fwrite(directory->data, page_size, 1, file);

    /* Write actual page to disk */
    fseeko(file, offset, SEEK_SET);
    fwrite(page->data, page_size, 1, file);
    fflush(file);

    /* Free up directory page */
    _free_page(directory);

    return true;
}

////////////////////////////////////////////////////////////////////////////////
// Heap Directory Iterator

HeapDirectoryIterator::HeapDirectoryIterator(Heapfile *heap) {
    heap_       = heap;
    directory_  = _init_page(heap, heapSchema);
    dir_offset_ = DIR_OFFSET;
}

HeapDirectoryIterator::~HeapDirectoryIterator() {
    _free_page(directory_);
}

int64_t HeapDirectoryIterator::offset() {
    return dir_offset_;
}

Page *HeapDirectoryIterator::next() {
    assert(hasNext());

    // Read the page
    fseeko(heap_->file_ptr, dir_offset_, SEEK_SET);
    fread(directory_->data, heap_->page_size, 1, heap_->file_ptr);

    // Read the directory header and assert consistency.
    Record headerRecord(heapSchema);
    assert(read_fixed_len_page(directory_, 0, &headerRecord, heapSchema));
    DirHdr *dirHeader = (DirHdr *) headerRecord.at(0);
    assert(dirHeader->offset == dir_offset_);
    assert(dirHeader->sig == DIRHDR_SIG);

    // Update the next directory offset. If the next offset is 0, change it to
    // -1 to mark completion. We need this because the initial directory has
    // offset 0.
    dir_offset_ = dirHeader->next;
    if (dir_offset_ == DIRHDR_NULL) {
        dir_offset_ = -1;
    }

    return directory_;
}

bool HeapDirectoryIterator::hasNext() {
    return dir_offset_ != -1;
}

////////////////////////////////////////////////////////////////////////////////
// Directory Page Iterator

DirectoryPageIterator::DirectoryPageIterator(Heapfile *heap, Page *directory,
                                             const Schema &schema)
        : schema_(schema), dir_iter_(directory, heapSchema) {
    heap_      = heap;
    directory_ = directory;
    page_      = _init_page(heap, schema);

    dir_iter_.next(); // Discard header.
}

DirectoryPageIterator::~DirectoryPageIterator() {
    _free_page(page_);
}

bool DirectoryPageIterator::hasNext() {
    return dir_iter_.hasNext();
}

Page *DirectoryPageIterator::next() {
    assert(dir_iter_.hasNext());

    Record dirRecord = dir_iter_.next();
    DirRecord *pageRecord = (DirRecord*) dirRecord.at(0);

    fseeko(heap_->file_ptr, pageRecord->offset, SEEK_SET);
    fread(page_->data, heap_->page_size, 1, heap_->file_ptr);

    return page_;
}

////////////////////////////////////////////////////////////////////////////////
// Record Iterator

RecordIterator::RecordIterator(Heapfile *heap, const Schema &schema)
        : schema_(schema), heap_iter_(heap) {
    heap_      = heap;
    dir_iter_  = NULL;
    page_iter_ = NULL;
}

RecordIterator::~RecordIterator() {
    if (dir_iter_)
        delete dir_iter_;
    if (page_iter_)
        delete page_iter_;
}

Record RecordIterator::next() {
    assert(hasNext());
    return page_iter_->next();
}

bool RecordIterator::hasNext() {
CHECK_PAGE:
    // Check if the current page has a record.
    if (page_iter_) {
        if (page_iter_->hasNext()) {
            return true;
        }

        // Current page has been exhausted. Clean up.
        delete page_iter_;
        page_iter_ = NULL;
    }

CHECK_DIRECTORY:
    // Check if the current directory has a page.
    if (dir_iter_) {
        if (dir_iter_->hasNext()) {
            Page *page = dir_iter_->next();
            page_iter_ = new PageRecordIterator(page, schema_);
            goto CHECK_PAGE;
        }

        // Current directory has been exhausted. Clean up.
        delete dir_iter_;
        dir_iter_ = NULL;
    }

// CHECK_HEAP:
    // Check if the heap has another directory.
    if (heap_iter_.hasNext()) {
        Page *dir = heap_iter_.next();
        dir_iter_ = new DirectoryPageIterator(heap_, dir, schema_);
        goto CHECK_DIRECTORY;
    }

    // Fully exhausted. No more records.
    return false;
}


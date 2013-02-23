#include <cassert>
#include <cstring>
#include <cstdio>
#include "heapmanager.h"

typedef struct {
    int offset;         /* Offset of current page */
    int next;           /* Offset of next page */
    unsigned int sig;   /* Magic number */
} DirHdr;

typedef struct {
    int pageId;
    int offset;
    int slots;
} DirRecord;

Schema heapSchema(3, 4);

inline PageID _get_new_id() {
    /**
     *  TODO: Make this global or store in heapfile. When reading new
     *  heapfile then traverse whole file and figure out the largest
     *  PageID and allocate subsequent ones from there.
     *
     */
    static PageID id = 0;
    return id++;
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
int _locate_pageId(Heapfile *heapfile, PageID pageId, Page **dirPage, int *dirSlot) {
    assert(dirPage != NULL);
    assert(dirSlot != NULL);

    FILE *file = heapfile->file_ptr;
    int page_size = heapfile->page_size;

    Page *directory = _init_page(heapfile);
    Record hdrRecord(heapSchema);
    DirHdr *dirHdr;

    int offset = -1, slot;

    int directory_offset, next = DIR_OFFSET;
    do {
        directory_offset = next;
        fseek(file, directory_offset, SEEK_SET);
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
void _init_dir_page(Page *directory, int offset) {
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

    /* Initialize primary directory page */
    Page *directory = _init_page(heapfile);

    /* If creating new heap then write primary directory page */
    if(newHeap) {
        _init_dir_page(directory, DIR_OFFSET);

        /* Write page to disk */
        fseek(file, DIR_OFFSET, SEEK_SET);
        fwrite(directory->data, page_size, 1, file);
        fflush(file);
    } else {
        /* Otherwise validate that heap file has proper directory record */
        fseek(file, DIR_OFFSET, SEEK_SET);
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
    fseek(file, 0, SEEK_END);
    int offset = ftell(file);
    PageID pageId = _get_new_id();
    Record dirRecord(heapSchema);
    DirRecord *pageRecord = (DirRecord *) dirRecord.at(0);

    pageRecord->pageId = pageId;
    pageRecord->offset = offset;
    pageRecord->slots = fixed_len_page_freeslots(page);

    /* Append page to file */
    fseek(file, offset, SEEK_SET);
    fwrite(page->data, page_size, 1, file);
    fflush(file);

    /**
     * Since we don't support page deletion, a new page can only be added
     * to the last directory page so traverse list to the last page and
     * try to add, if it is full then we need to create a new directory
     * page and link it.
     */
    Page *directory = _init_page(heapfile);
    Record hdrRecord(heapSchema);
    DirHdr *dirHdr;

    int directory_offset, next = DIR_OFFSET;
    do {
        /* Load directory page */
        directory_offset = next;
        fseek(file, directory_offset, SEEK_SET);
        fread(directory->data, page_size, 1, file);

        assert(true == read_fixed_len_page(directory, 0, &hdrRecord, heapSchema));

        dirHdr = (DirHdr *) (hdrRecord.at(0));
        assert(dirHdr->offset == directory_offset);
        assert(dirHdr->sig == DIRHDR_SIG);

        /* Set next pointer */
        next = dirHdr->next;
    } while(next != DIRHDR_NULL);

    /**
     * At this point should be at the last directory page, try adding record
     */
    if((slot = add_fixed_len_page(directory, &dirRecord, heapSchema)) == -1) {
        /**
         * Last directory page was full so allocate new one and link it
         */
        fseek(file, 0, SEEK_END);
        int new_directory_offset = ftell(file);

        /* Update header in current directory page and write it */
        dirHdr->next = new_directory_offset;
        write_fixed_len_page(directory, 0, &hdrRecord, heapSchema);

        fseek(file, directory_offset, SEEK_SET);
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
    fseek(file, directory_offset, SEEK_SET);
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
    int offset, slot;

    /* If record for given PageID doesn't exist then fail */
    if((offset = _locate_pageId(heapfile, pid, &directory, &slot)) == -1) {
        return false;
    }

    /* Read page from disk */
    fseek(heapfile->file_ptr, offset, SEEK_SET);
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
    int offset, slot;

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

    fseek(file, dirHdr->offset, SEEK_SET);
    fwrite(directory->data, page_size, 1, file);

    /* Write actual page to disk */
    fseek(file, offset, SEEK_SET);
    fwrite(page->data, page_size, 1, file);
    fflush(file);

    /* Free up directory page */
    _free_page(directory);

    return true;
}

RecordIterator::RecordIterator(Heapfile *heap, const Schema &schema)
        : schema_(schema) {
    heap_           = heap;
    page_           = _init_page(heap, schema_);
    directory_      = _init_page(heap, heapSchema);
    page_iter_      = 0;
    directory_iter_ = 0;

    // Read the first directory.
    fseek(heap_->file_ptr, DIR_OFFSET, SEEK_SET);
    fread(directory_->data, heap_->page_size, 1, heap_->file_ptr);
}

Record RecordIterator::next() {
    // Can assume that next page exists
    if (!page_iter_->hasNext()) {
        // TODO Load next page
    }

    return page_iter_->next();
}

bool RecordIterator::hasNext() {
    // TODO
    return false;
}

RecordIterator::~RecordIterator() {
    _free_page(directory_);
    _free_page(page_);
}


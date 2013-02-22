#include <cassert>
#include <cstring>
#include <cstdio>
#include "heapmanager.h"

typedef struct {
    int next;
    unsigned int sig1;
    unsigned int sig2;
} DirHdr;

typedef struct {
    int pageId;
    int offset;
    int slots;
} DirRecord;

Schema heapSchema(3, 4);

inline PageID _get_new_id() {
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
        /* First directory entry of a heap page is a pointer to next or NULL */
        Record hdrRecord(heapSchema);
        DirHdr *dirHdr = (DirHdr *) (hdrRecord.at(0));
        dirHdr->next = DIRHDR_NULL;
        dirHdr->sig1 = DIRHDR_SIG1;
        dirHdr->sig2 = DIRHDR_SIG2;

        /* Populate directory header and make sure it's at the first slot */
        assert(0 == add_fixed_len_page(directory, &hdrRecord, heapSchema));

        /* Write page to disk */
        fseek(file, 0, SEEK_SET);
        fwrite(directory->data, page_size, 1, file);
        fflush(file);
    } else {
        /* Otherwise validate that heap file has proper directory record */
        fseek(file, 0, SEEK_SET);
        fread(directory->data, page_size, 1, file);

        Record hdrRecord(heapSchema);
        assert(true == read_fixed_len_page(directory, 0, &hdrRecord, heapSchema));

        DirHdr *dirHdr = (DirHdr *) (hdrRecord.at(0));
        assert(dirHdr->sig1 == DIRHDR_SIG1);
        assert(dirHdr->sig2 == DIRHDR_SIG2);
    }

    /* Release memory for pages */
    _free_page(directory);
}

/**
 * Allocate another page in the heapfile.  This grows the file by a page.
 *
 * TODO: Support overflows by linking directory pages.
 */
PageID alloc_page(Heapfile *heapfile) {
    FILE *file = heapfile->file_ptr;
    int page_size = heapfile->page_size;
    int slot;

    /* Allocate page */
    Page *page = _init_page(heapfile, csvSchema);

    /* Create directory record */
    fseek(file, 0, SEEK_END);
    int offset = ftell(file);
    PageID pageId = _get_new_id();
    Record dirRecord(heapSchema);
    DirRecord *pageRecord = (DirRecord *) dirRecord.at(0);

    pageRecord->pageId = pageId;
    pageRecord->offset = offset;
    pageRecord->slots = fixed_len_page_freeslots(page);

    /* Load primary directory and add page record */
    Page *directory = _init_page(heapfile);
    fseek(file, 0, SEEK_SET);
    fread(directory->data, page_size, 1, file);

    if((slot = add_fixed_len_page(directory, &dirRecord, heapSchema)) == -1)
        return -1;

    /* Write directory to disk */
    fseek(file, 0, SEEK_SET);
    fwrite(directory->data, page_size, 1, file);

    /* Append page to file */
    fseek(file, offset, SEEK_SET);
    fwrite(page->data, page_size, 1, file);
    fflush(file);

    /* Free pages */
    _free_page(page);
    _free_page(directory);

    return pageId;
}

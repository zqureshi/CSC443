#include <cassert>
#include <cstring>
#include "heapmanager.h"

typedef struct {
    int next;
    long long signature;
} DirHdr;

Schema heapSchema(3, 4);

inline int _get_new_id() {
    static int id = 0;
    return id++;
}

/**
 * Initalize a heapfile to use the file and page size given.
 */
void init_heapfile(Heapfile *heapfile, int page_size, FILE *file, bool newHeap) {
    /* Set file pointer and page sizes */
    heapfile->file_ptr = file;
    heapfile->page_size = page_size;

    /* Initialize primary directory page */
    Page directory;
    init_fixed_len_page(&directory, page_size, heapSchema.numAttrs * heapSchema.attrLen);

    /* If creating new heap then write primary directory page */
    if(newHeap) {
        /* First directory entry of a heap page is a pointer to next or NULL */
        Record hdrRecord(heapSchema);
        DirHdr *dirHdr = (DirHdr *) (hdrRecord.at(0));
        dirHdr->next = DIRHDR_NULL;
        dirHdr->signature = DIRHDR_SIGNATURE;

        /* Populate directory header and make sure it's at the first slot */
        assert(0 == add_fixed_len_page(&directory, &hdrRecord, heapSchema));

        /* Write page to disk */
        fseek(file, 0, SEEK_SET);
        fwrite(directory.data, page_size, 1, file);
        fflush(file);
    } else {
        /* Otherwise validate that heap file has proper directory record */
        fseek(file, 0, SEEK_SET);
        fread(directory.data, page_size, 1, file);

        Record hdrRecord(heapSchema);
        assert(true == read_fixed_len_page(&directory, 0, &hdrRecord, heapSchema));

        DirHdr *dirHdr = (DirHdr *) (hdrRecord.at(0));
        assert(dirHdr->signature == DIRHDR_SIGNATURE);
    }

    /* Release memory for pages */
    delete [] (char *) (directory.data);
}

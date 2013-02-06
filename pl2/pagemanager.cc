#include <iostream>
#include <cstring>
#include "pagemanager.h"

using namespace std;

inline int _capacity(Page *page) {
    return page->page_size/(sizeof(char) + page->slot_size);
}

void *_slot_offset(Page *page, int slot) {
    return (char *) page->data + _capacity(page) + page->slot_size * slot;
}

/**
 * Initializes a page using the given slot size
 */
void init_fixed_len_page(Page *page, int page_size, int slot_size) {
    /* Set page and slot sizes */
    page->page_size = page_size;
    page->slot_size = slot_size;

    /* Allocate memory and initialize slot directory */
    page->data = new char[page_size];
    memset(page->data, 0, _capacity(page));
}

/**
 * Calculates the maximal number of records that fit in a page
 */
int fixed_len_page_capacity(Page *page) {
    return _capacity(page);
}

/**
 * Calculate the free space (number of free slots) in the page
 */
int fixed_len_page_freeslots(Page *page) {
    char *directory = (char *) page->data;
    int slots = 0;
    for(int i = 0; i < _capacity(page); i++)
        slots += 1 - directory[i];
    return slots;
}

/**
 * Add a record to the page
 * Returns:
 *   record slot offset if successful,
 *   -1 if unsuccessful (page full)
 */
int add_fixed_len_page(Page *page, Record *r) {
    /* If slot is smaller than serialized record we can't store it */
    if(fixed_len_sizeof(r) > page->slot_size) {
        return -1;
    }

    char *directory = (char *) page->data;
    int slot = -1;
    for(int i = 0; i < _capacity(page); i++) {
        if(directory[i] == 0) {
            directory[i] = 1;
            return slot;
        }
    }

    return slot;
}

/**
 * Write a record into a given slot.
 */
void write_fixed_len_page(Page *page, int slot, Record *r) {
    fixed_len_write(r, _slot_offset(page, slot));
}

/**
 * Read a record from the page from a given slot.
 */
void read_fixed_len_page(Page *page, int slot, Record *r) {
    fixed_len_read(_slot_offset(page, slot), fixed_len_sizeof(r),r);
}

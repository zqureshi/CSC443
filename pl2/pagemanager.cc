#include <iostream>
#include <cstring>
#include "pagemanager.h"

using namespace std;

inline int _fixed_page_len_capacity(Page *page) {
    return page->page_size/(sizeof(char) + page->slot_size);
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
    memset(page->data, 0, _fixed_page_len_capacity(page));
}

/**
 * Calculates the maximal number of records that fit in a page
 */
int fixed_len_page_capacity(Page *page) {
    return _fixed_page_len_capacity(page);
}

/**
 * Calculate the free space (number of free slots) in the page
 */
int fixed_len_page_freeslots(Page *page) {
    char *directory = (char *) page->data;
    int slots = 0;
    for(int i = 0; i < _fixed_page_len_capacity(page); i++)
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
    // TODO: Implement Method
    return 0;
}

/**
 * Write a record into a given slot.
 */
void write_fixed_len_page(Page *page, int slot, Record *r) {
    // TODO: Implement Method
}

/**
 * Read a record from the page from a given slot.
 */
void read_fixed_len_page(Page *page, int slot, Record *r) {
    // TODO: Implement Method
}

#include "pagemanager.h"

using namespace std;

inline int _fixed_page_len_capacity(int page_size, int slot_size) {
    return page_size/(sizeof(char) + slot_size);
}

/**
 * Initializes a page using the given slot size
 */
void init_fixed_len_page(Page *page, int page_size, int slot_size) {
    // TODO: Implement Method
}

/**
 * Calculates the maximal number of records that fit in a page
 */
int fixed_len_page_capacity(Page *page) {
    return _fixed_page_len_capacity(page->page_size, page->slot_size);
}

/**
 * Calculate the free space (number of free slots) in the page
 */
int fixed_len_page_freeslots(Page *page) {
    // TODO: Implement Method
    return 0;
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

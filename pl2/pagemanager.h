#ifndef PAGEMANAGER_H
#define PAGEMANAGER_H

#include "serializer.h"

typedef struct {
    void *data;
    int page_size;
    int slot_size;
} Page;

/**
 * Initializes a page using the given slot size
 */
void init_fixed_len_page(Page *page, int page_size, int slot_size);

/**
 * Calculates the maximal number of records that fit in a page
 */
int fixed_len_page_capacity(Page *page);

/**
 * Calculate the free space (number of free slots) in the page
 */
int fixed_len_page_freeslots(Page *page);

/**
 * Add a record to the page
 * Returns:
 *   record slot offset if successful,
 *   -1 if unsuccessful (page full)
 */
int add_fixed_len_page(Page *page, Record *r, Schema schema = csvSchema);

/**
 * Write a record into a given slot.
 */
void write_fixed_len_page(Page *page, int slot, Record *r, Schema schema = csvSchema);

/**
 * Read a record from the page from a given slot.
 */
bool read_fixed_len_page(Page *page, int slot, Record *r, Schema schema = csvSchema);

#endif

#include <cstring>
#include <cassert>
#include <string>
#include "serializer.h"

using namespace std;

#define SCHEMA_NUM_ATTRS 100
#define SCHEMA_ATTR_LEN 10
#define SCHEMA_ATTR_SIZE SCHEMA_ATTR_LEN * sizeof(char)

/**
 * Allocate a Record and return pointer to it
 */
Record *alloc_record() {
    Record *record = new Record(SCHEMA_NUM_ATTRS);
    for(int i = 0; i < SCHEMA_NUM_ATTRS; i++) {
        record->at(i) = new char[SCHEMA_ATTR_LEN];
        memset((char *) record->at(i), '\0', SCHEMA_ATTR_LEN);
    }
    return record;
}

/**
 * Free up record and its associated pointers.
 */
void free_record(Record *record) {
    for(unsigned int i = 0; i < record->size(); i++)
        delete [] record->at(i);
    delete record;
}

/**
 * Inline implementation of fixed_len_sizeof()
 */
inline int _fixed_len_sizeof(Record *record) {
    return SCHEMA_NUM_ATTRS * SCHEMA_ATTR_SIZE;
}

/**
 * Compute the number of bytes required to serialize record
 */
int fixed_len_sizeof(Record *record) {
    return _fixed_len_sizeof(record);
}

/**
 * Serialize the record to a byte array to be stored in buf.
 *
 * Precondition: The *buf is already allocated with enough space to store
 * number of bytes returned by fixed_len_sizeof()
 */
void fixed_len_write(Record *record, void *buf) {
    for(Record::iterator i = record->begin(); i != record->end(); i++) {
        memcpy(buf, *i, SCHEMA_ATTR_SIZE);
        buf = (char *) buf + SCHEMA_ATTR_SIZE;
    }
}

/**
 * Deserializes from `size` bytes from the buffer, `buf`, and
 * stores the record in `record`.
 */
void fixed_len_read(void *buf, int size, Record *record) {
    assert(size == _fixed_len_sizeof(record));
    for(int i = 0; i < SCHEMA_NUM_ATTRS; i++) {
        memcpy((char *) record->at(i), (char *) buf + SCHEMA_ATTR_SIZE * i, SCHEMA_ATTR_SIZE);
    }
}

/**
 * Compute the number of bytes required to serialize record
 */
int var_len_sizeof(Record *record) {
    /* Fixed header overhead */
    int length = sizeof(int) * SCHEMA_NUM_ATTRS;
    for(int i = 0; i < SCHEMA_NUM_ATTRS; i++) {
        length += strlen(record->at(i));
    }
    return length;
}

/**
 *  the record using variable record encoding
 */
void var_len_write(Record *record, void *buf) {
}

/**
 * Deserialize the `buf` which contains the variable record encoding.
 */
void var_len_read(void *buf, int size, Record *record) {
}

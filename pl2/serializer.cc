#include <cstring>
#include <cassert>
#include <string>
#include "serializer.h"

using namespace std;

#define SCHEMA_NUM_ATTRS 100
#define SCHEMA_ATTR_LEN 10
#define SCHEMA_ATTR_SIZE SCHEMA_ATTR_LEN * sizeof(char)

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
    return 0;
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

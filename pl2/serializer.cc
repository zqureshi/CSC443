#include <cstring>
#include <cassert>
#include <vector>
#include "serializer.h"
#include "constants.h"

/**
 * Allocate a Record
 */
Record::Record() : std::vector<V>(SCHEMA_NUM_ATTRS) {
    for (size_t i = 0; i < SCHEMA_NUM_ATTRS; ++i) {
        at(i) = new char[SCHEMA_ATTR_LEN];
        memset((char*) at(i), 0, SCHEMA_ATTR_LEN);
    }
}

/**
 * Free up record and its associated pointers.
 */
Record::~Record() {
    for (size_t i = 0; i < size(); ++i)
        delete[] at(i);
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
    int length = SCHEMA_HDR_SIZE;
    for(int i = 0; i < SCHEMA_NUM_ATTRS; i++) {
        length += strlen(record->at(i));
    }
    return length;
}

/**
 *  the record using variable record encoding
 */
void var_len_write(Record *record, void *buf) {
    int *header = (int *) buf;
    char *data = (char *) header + SCHEMA_HDR_SIZE;
    for(int i = 0; i < SCHEMA_NUM_ATTRS; i++) {
        int len = strlen(record->at(i));
        header[i] = (int) (data - (char *) header);
        memcpy(data, record->at(i), len);
        data += len;
    }
    header[SCHEMA_NUM_ATTRS] = (int) (data - (char *) header);
}

/**
 * Deserialize the `buf` which contains the variable record encoding.
 */
void var_len_read(void *buf, int size, Record *record) {
    int *header = (int *) buf;
    for(int i = 0; i < SCHEMA_NUM_ATTRS; i++) {
        memcpy((char *) record->at(i), (char *) buf + header[i], header[i+1] - header[i]);
        memset((char *) record->at(i) + header[i+1] - header[i], '\0', 1);
    }
}

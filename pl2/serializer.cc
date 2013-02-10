#include <cstring>
#include <cassert>
#include <vector>
#include "serializer.h"
#include "constants.h"

Schema csvSchema(100, 10);

/**
 * Initialize Schema.
 */
Schema::Schema(int numAttrs, int attrLen) {
    this->numAttrs = numAttrs;
    this->attrLen = attrLen;
    this->hdrSize = sizeof(int) * (numAttrs + 1);
}

/**
 * Allocate a Record
 */
Record::Record(Schema schema) : std::vector<V>(schema.numAttrs) {
    char *block = new char[schema.numAttrs * schema.attrLen];
    memset(block, 0, schema.numAttrs * schema.attrLen);

    for (int i = 0; i < schema.numAttrs; ++i) {
        at(i) = block + schema.attrLen * i;
    }
}

/**
 * Free up record and its associated pointers.
 */
Record::~Record() {
    delete [] at(0);
}

/**
 * Inline implementation of fixed_len_sizeof()
 */
inline int _fixed_len_sizeof(Record *record, Schema schema) {
    return schema.numAttrs * schema.attrLen;
}

/**
 * Compute the number of bytes required to serialize record
 */
int fixed_len_sizeof(Record *record, Schema schema) {
    return _fixed_len_sizeof(record, schema);
}

/**
 * Serialize the record to a byte array to be stored in buf.
 *
 * Precondition: The *buf is already allocated with enough space to store
 * number of bytes returned by fixed_len_sizeof()
 */
void fixed_len_write(Record *record, void *buf, Schema schema) {
    for(Record::iterator i = record->begin(); i != record->end(); i++) {
        memcpy(buf, *i, schema.attrLen);
        buf = (char *) buf + schema.attrLen;
    }
}

/**
 * Deserializes from `size` bytes from the buffer, `buf`, and
 * stores the record in `record`.
 */
void fixed_len_read(void *buf, int size, Record *record, Schema schema) {
    assert(size == _fixed_len_sizeof(record, schema));
    for(int i = 0; i < schema.numAttrs; i++) {
        memcpy((char *) record->at(i), (char *) buf + schema.attrLen * i, schema.attrLen);
    }
}

/**
 * Compute the number of bytes required to serialize record
 */
int var_len_sizeof(Record *record, Schema schema) {
    /* Fixed header overhead */
    int length = schema.hdrSize;
    for(int i = 0; i < schema.numAttrs; i++) {
        length += strlen(record->at(i));
    }
    return length;
}

/**
 *  the record using variable record encoding
 */
void var_len_write(Record *record, void *buf, Schema schema) {
    int *header = (int *) buf;
    char *data = (char *) header + schema.hdrSize;
    for(int i = 0; i < schema.numAttrs; i++) {
        int len = strlen(record->at(i));
        header[i] = (int) (data - (char *) header);
        memcpy(data, record->at(i), len);
        data += len;
    }
    header[schema.numAttrs] = (int) (data - (char *) header);
}

/**
 * Deserialize the `buf` which contains the variable record encoding.
 */
void var_len_read(void *buf, int size, Record *record, Schema schema) {
    int *header = (int *) buf;
    for(int i = 0; i < schema.numAttrs; i++) {
        memcpy((char *) record->at(i), (char *) buf + header[i], header[i+1] - header[i]);
        memset((char *) record->at(i) + header[i+1] - header[i], '\0', 1);
    }
}

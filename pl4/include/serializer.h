#ifndef SERIALIZER_H
#define SERIALIZER_H

#include <vector>
#include "constants.h"

typedef const char * V;

/**
 * Schema of a tuple with all attributes of same length.
 */
struct Schema {
    int numAttrs;
    int attrLen;
    int hdrSize;
    Schema(int numAttrs, int attrLen);
};

extern Schema csvSchema;

/**
 * A Record is a vector of `V` values.
 */
class Record : public std::vector<V> {
    public:
        Record(const Schema& = csvSchema);
        Record(const Record &record);
        ~Record();
};

/**
 * Compute the number of bytes required to serialize record
 */
int fixed_len_sizeof(Record *record, const Schema& = csvSchema);

/**
 * Serialize the record to a byte array to be stored in buf.
 */
void fixed_len_write(Record *record, void *buf, const Schema& = csvSchema);

/**
 * Deserializes from `size` bytes from the buffer, `buf`, and
 * stores the record in `record`.
 */
void fixed_len_read(void *buf, int size, Record *record, const Schema& = csvSchema);

/**
 * Compute the number of bytes required to serialize record
 */
int var_len_sizeof(Record *record, const Schema& = csvSchema);

/**
 * Serialize the record using variable record encoding
 */
void var_len_write(Record *record, void *buf, const Schema& = csvSchema);

/**
 * Deserialize the `buf` which contains the variable record encoding.
 */
void var_len_read(void *buf, int size, Record *record, const Schema& = csvSchema);

#endif

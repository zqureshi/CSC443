#ifndef SERIALIZER_H
#define SERIALIZER_H

#include <vector>
typedef const char * V;
typedef std::vector<V> Record;

/**
 * Allocate a Record and return pointer to it
 */
Record *alloc_record();

/**
 * Free up record and its associated pointers.
 */
void free_record(Record *record);

/**
 * Compute the number of bytes required to serialize record
 */
int fixed_len_sizeof(Record *record);

/**
 * Serialize the record to a byte array to be stored in buf.
 */
void fixed_len_write(Record *record, void *buf);

/**
 * Deserializes from `size` bytes from the buffer, `buf`, and
 * stores the record in `record`.
 */
void fixed_len_read(void *buf, int size, Record *record);

/**
 * Compute the number of bytes required to serialize record
 */
int var_len_sizeof(Record *record);

/**
 * Serialize the record using variable record encoding
 */
void var_len_write(Record *record, void *buf);

/**
 * Deserialize the `buf` which contains the variable record encoding.
 */
void var_len_read(void *buf, int size, Record *record);

#endif

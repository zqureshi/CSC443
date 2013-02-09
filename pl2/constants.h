#ifndef CONSTANTS_H_
#define CONSTANTS_H_

/* Serialization Constants */
#define SCHEMA_NUM_ATTRS 100
#define SCHEMA_HDR_SIZE sizeof(int) * (SCHEMA_NUM_ATTRS + 1)
#define SCHEMA_ATTR_LEN 10
#define SCHEMA_ATTR_SIZE SCHEMA_ATTR_LEN * sizeof(char)

#endif /* CONSTANTS_H_ */

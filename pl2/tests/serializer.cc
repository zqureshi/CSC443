#include <iostream>
#include <cstring>
#include "serializer.h"
#include <gtest/gtest.h>

using namespace std;

#define SCHEMA_NUM_ATTRS 100
#define SCHEMA_ATTR_LEN 10
#define SCHEMA_ATTR_SIZE SCHEMA_ATTR_LEN * sizeof(char)

TEST(FixedSerializer, RecordLengthTest) {
    Record r;
    ASSERT_EQ(1000, fixed_len_sizeof(&r)) << "Calculated record length doesn't match schema";
}

TEST(FixedSerializer, SerializationTest) {
    Record *record = alloc_record();

    /* Populate record */
    for(int i = 0; i < SCHEMA_NUM_ATTRS; i++) {
        sprintf((char *) record->at(i), "%9d", i);
    }

    /* Serialize to char array */
    char *buf = new char[fixed_len_sizeof(record)];
    fixed_len_write(record, buf);

    /* Check whole blob */
    for(int i = 0; i < SCHEMA_NUM_ATTRS; i++) {
        ASSERT_STREQ(record->at(i), buf + i * SCHEMA_ATTR_LEN);
    }

    /* Free up memory */
    delete [] buf;
    free_record(record);
}

TEST(VariableSerializer, EmptyRecordLengthTest) {
    Record *record = alloc_record();
    ASSERT_EQ(sizeof(int) * SCHEMA_NUM_ATTRS, var_len_sizeof(record));
    free_record(record);
}

TEST(VariableSerializer, SparseRecordLengthTest) {
    Record *record = alloc_record();
    strcpy((char *) record->at(0), "hello");
    ASSERT_EQ(sizeof(int) * SCHEMA_NUM_ATTRS + strlen("hello"), var_len_sizeof(record));
    free_record(record);
}

TEST(VariableSerializer, FullRecordLengthTest) {
    Record *record = alloc_record();

    /* Populate record */
    for(int i = 0; i < SCHEMA_NUM_ATTRS; i++) {
        sprintf((char *) record->at(i), "%9d", i);
    }

    ASSERT_EQ(sizeof(int) * SCHEMA_NUM_ATTRS + 900, var_len_sizeof(record));
    free_record(record);
}

int main(int argc, char *argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

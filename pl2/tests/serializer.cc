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
    ASSERT_EQ(sizeof(int) * SCHEMA_NUM_ATTRS, (unsigned int) var_len_sizeof(record));
    free_record(record);
}

TEST(VariableSerializer, SparseRecordLengthTest) {
    Record *record = alloc_record();
    strcpy((char *) record->at(0), "hello");
    ASSERT_EQ(sizeof(int) * SCHEMA_NUM_ATTRS + strlen("hello"), (unsigned int) var_len_sizeof(record));
    free_record(record);
}

TEST(VariableSerializer, FullRecordLengthTest) {
    Record *record = alloc_record();

    /* Populate record */
    for(int i = 0; i < SCHEMA_NUM_ATTRS; i++) {
        sprintf((char *) record->at(i), "%9d", i);
    }

    ASSERT_EQ(sizeof(int) * SCHEMA_NUM_ATTRS + 900, (unsigned int) var_len_sizeof(record));
    free_record(record);
}

void variable_serialization_test(void (*populator)(Record *)) {
    Record *recordIn = alloc_record(), *recordOut = alloc_record();

    /* Populate Record */
    populator(recordIn);

    /* Serialize Record */
    int recordLen = var_len_sizeof(recordIn);
    char *buf = new char[recordLen];
    var_len_write(recordIn, buf);

    /* Deserialize Record */
    var_len_read(buf, recordLen, recordOut);

    /* Check Values */
    ASSERT_EQ(recordLen, var_len_sizeof(recordOut));
    for(int i = 0; i < SCHEMA_NUM_ATTRS; i++) {
        ASSERT_STREQ(recordIn->at(i), recordOut->at(i));
    }

    delete [] buf;
    free_record(recordIn);
    free_record(recordOut);
}

void populate_full_record(Record *recordIn) {
    for(int i = 0; i < SCHEMA_NUM_ATTRS; i++) {
        sprintf((char *) recordIn->at(i), "%d", i);
    }
}

void populate_sparse_record(Record *recordIn) {
    strcpy((char *) recordIn->at(0), "hello");
}

void populate_empty_record(Record *recordIn) {
}

TEST(VariableSerializer, RoundtripSerializationTest) {
    variable_serialization_test(&populate_full_record);
}

TEST(VariableSerializer, SparseRecordRoundtripSerializationTest) {
    variable_serialization_test(&populate_sparse_record);
}

TEST(VariableSerializer, EmptyRecordRoundTripSerializationTest) {
    variable_serialization_test(&populate_empty_record);
}

int main(int argc, char *argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

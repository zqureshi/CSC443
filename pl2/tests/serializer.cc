#include <iostream>
#include <cstring>
#include "serializer.h"
#include "constants.h"
#include <gtest/gtest.h>

using namespace std;

TEST(FixedSerializer, RecordLengthTest) {
    Record r;
    ASSERT_EQ(1000, fixed_len_sizeof(&r)) << "Calculated record length doesn't match schema";
}

TEST(FixedSerializer, SerializationTest) {
    Record *record = new Record();

    /* Populate record */
    for(int i = 0; i < csvSchema.numAttrs; i++) {
        sprintf((char *) record->at(i), "%9d", i);
    }

    /* Serialize to char array */
    char *buf = new char[fixed_len_sizeof(record)];
    fixed_len_write(record, buf);

    /* Check whole blob */
    for(int i = 0; i < csvSchema.numAttrs; i++) {
        ASSERT_STREQ(record->at(i), buf + i * csvSchema.attrLen);
    }

    /* Free up memory */
    delete [] buf;
    delete record;
}

TEST(VariableSerializer, EmptyRecordLengthTest) {
    Record *record = new Record();
    ASSERT_EQ(csvSchema.hdrSize, var_len_sizeof(record));
    delete record;
}

TEST(VariableSerializer, SparseRecordLengthTest) {
    Record *record = new Record();
    strcpy((char *) record->at(0), "hello");
    ASSERT_EQ(csvSchema.hdrSize + strlen("hello"), (unsigned int) var_len_sizeof(record));
    delete record;
}

TEST(VariableSerializer, FullRecordLengthTest) {
    Record *record = new Record();

    /* Populate record */
    for(int i = 0; i < csvSchema.numAttrs; i++) {
        sprintf((char *) record->at(i), "%9d", i);
    }

    ASSERT_EQ(csvSchema.hdrSize + 900, var_len_sizeof(record));
    delete record;
}

void variable_serialization_test(void (*populator)(Record *)) {
    Record *recordIn = new Record(), *recordOut = new Record();

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
    for(int i = 0; i < csvSchema.numAttrs; i++) {
        ASSERT_STREQ(recordIn->at(i), recordOut->at(i));
    }

    delete [] buf;
    delete recordIn;
    delete recordOut;
}

void populate_full_record(Record *recordIn) {
    for(int i = 0; i < csvSchema.numAttrs; i++) {
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

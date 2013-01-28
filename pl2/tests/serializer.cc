#include "serializer.h"
#include <gtest/gtest.h>

TEST(FixedSerializer, RecordLength) {
    Record r;
    ASSERT_EQ(1000, fixed_len_sizeof(&r)) << "Calculated record length doesn't match schema";
}

int main(int argc, char *argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

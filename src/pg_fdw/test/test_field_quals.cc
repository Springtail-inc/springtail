#include <gtest/gtest.h>

#include <common/init.hh>
#include <common/json.hh>

#include <pg_fdw/pg_fdw_mgr.hh>

using namespace springtail;
using namespace springtail::pg_fdw;

namespace {

    class PgFdwCheckTypeCompatibilityTest : public ::testing::Test {
    protected:
        void SetUp() override {
            qual = new ConstQual();
            qual->base.op = EQUALS;
            qual->base.isArray = false;
            qual->base.useOr = false;
        }

        void TearDown() override {
            delete qual;
        }

        ConstQualPtr qual;
        SchemaColumn column;
    };

    TEST_F(PgFdwCheckTypeCompatibilityTest, Int8ToInt16) {
        qual->base.typeoid = CHAROID;  // INT8
        qual->value = Int8GetDatum(100);

        column.type = SchemaType::INT16;
        column.pg_type = INT2OID;

        EXPECT_TRUE(PgFdwMgr::check_type_compatibility(column, qual));
        EXPECT_EQ(qual->base.typeoid, INT2OID);
        EXPECT_EQ(DatumGetInt16(qual->value), 100);
    }

    TEST_F(PgFdwCheckTypeCompatibilityTest, Int16ToInt32) {
        qual->base.typeoid = INT2OID;  // INT16
        qual->value = Int16GetDatum(1000);

        column.type = SchemaType::INT32;
        column.pg_type = INT4OID;

        EXPECT_TRUE(PgFdwMgr::check_type_compatibility(column, qual));
        EXPECT_EQ(qual->base.typeoid, INT4OID);
        EXPECT_EQ(DatumGetInt32(qual->value), 1000);
    }

    TEST_F(PgFdwCheckTypeCompatibilityTest, Int32ToInt64) {
        qual->base.typeoid = INT4OID;  // INT32
        qual->value = Int32GetDatum(100000);

        column.type = SchemaType::INT64;
        column.pg_type = INT8OID;

        EXPECT_TRUE(PgFdwMgr::check_type_compatibility(column, qual));
        EXPECT_EQ(qual->base.typeoid, INT8OID);
        EXPECT_EQ(DatumGetInt64(qual->value), 100000);
    }

    TEST_F(PgFdwCheckTypeCompatibilityTest, Int64ToInt32Valid) {
        qual->base.typeoid = INT8OID;  // INT64
        qual->value = Int64GetDatum(100000);

        column.type = SchemaType::INT32;
        column.pg_type = INT4OID;

        EXPECT_TRUE(PgFdwMgr::check_type_compatibility(column, qual));
        EXPECT_EQ(qual->base.typeoid, INT4OID);
        EXPECT_EQ(DatumGetInt32(qual->value), 100000);
    }

    TEST_F(PgFdwCheckTypeCompatibilityTest, Int64ToInt32Invalid) {
        qual->base.typeoid = INT8OID;  // INT64
        qual->value = Int64GetDatum(3000000000LL);  // Value too large for INT32

        column.type = SchemaType::INT32;
        column.pg_type = INT4OID;

        EXPECT_FALSE(PgFdwMgr::check_type_compatibility(column, qual));
    }

    TEST_F(PgFdwCheckTypeCompatibilityTest, Float32ToFloat64) {
        qual->base.typeoid = FLOAT4OID;  // FLOAT32
        qual->value = Float4GetDatum(123.456f);

        column.type = SchemaType::FLOAT64;
        column.pg_type = FLOAT8OID;

        EXPECT_TRUE(PgFdwMgr::check_type_compatibility(column, qual));
        EXPECT_EQ(qual->base.typeoid, FLOAT8OID);
        EXPECT_FLOAT_EQ(DatumGetFloat8(qual->value), 123.456);
    }

    TEST_F(PgFdwCheckTypeCompatibilityTest, Float64ToFloat32Valid) {
        qual->base.typeoid = FLOAT8OID;  // FLOAT64
        qual->value = Float8GetDatum(123.456);

        column.type = SchemaType::FLOAT32;
        column.pg_type = FLOAT4OID;

        EXPECT_FALSE(PgFdwMgr::check_type_compatibility(column, qual));
        //EXPECT_EQ(qual->base.typeoid, FLOAT4OID);
        //EXPECT_FLOAT_EQ(DatumGetFloat4(qual->value), 123.456f);
    }

    TEST_F(PgFdwCheckTypeCompatibilityTest, Float64ToFloat32Invalid) {
        qual->base.typeoid = FLOAT8OID;  // FLOAT64
        // Use a value that can't be precisely represented in float32
        qual->value = Float8GetDatum(123.456789012345);

        column.type = SchemaType::FLOAT32;
        column.pg_type = FLOAT4OID;

        EXPECT_FALSE(PgFdwMgr::check_type_compatibility(column, qual));
    }

    TEST_F(PgFdwCheckTypeCompatibilityTest, Int32ToFloat64) {
        qual->base.typeoid = INT4OID;  // INT32
        qual->value = Int32GetDatum(123456);

        column.type = SchemaType::FLOAT64;
        column.pg_type = FLOAT8OID;

        EXPECT_TRUE(PgFdwMgr::check_type_compatibility(column, qual));
        EXPECT_EQ(qual->base.typeoid, FLOAT8OID);
        EXPECT_DOUBLE_EQ(DatumGetFloat8(qual->value), 123456.0);
    }

    TEST_F(PgFdwCheckTypeCompatibilityTest, Int64ToFloat32Valid) {
        qual->base.typeoid = INT8OID;  // INT64
        qual->value = Int64GetDatum(123456);

        column.type = SchemaType::FLOAT32;
        column.pg_type = FLOAT4OID;

        EXPECT_TRUE(PgFdwMgr::check_type_compatibility(column, qual));
        EXPECT_EQ(qual->base.typeoid, FLOAT4OID);
        EXPECT_FLOAT_EQ(DatumGetFloat4(qual->value), 123456.0f);
    }

    TEST_F(PgFdwCheckTypeCompatibilityTest, Int64ToFloat32Invalid) {
        qual->base.typeoid = INT8OID;  // INT64
        qual->value = Int64GetDatum(16777217);

        column.type = SchemaType::FLOAT32;
        column.pg_type = FLOAT4OID;

        EXPECT_FALSE(PgFdwMgr::check_type_compatibility(column, qual));
    }
}
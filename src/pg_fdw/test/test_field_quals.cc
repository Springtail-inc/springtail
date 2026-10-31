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

    /**
     * Qual-recognition tests for the GiST/GIN qual routing seam added in the btree redesign's
     * phase 12 (see phases/phase-12.md's Design notes). GIN_SIMILAR ("%", pg_trgm's similarity
     * operator) must never be picked up by the ordinary btree qual-matching path
     * (_is_type_sortable()/_get_index_quals()) the way EQUALS/NOT_EQUALS/etc. are -- that path
     * feeds MergeTable::lower_bound()/upper_bound(), which only resolves a btree-shaped
     * secondary index (Table::index()), not a GiST/GIN one (Table::gist_index()/gin_index()).
     * GIN_SIMILAR's own routing (PgFdwMgr::_try_init_gin_scan()) is exercised by this phase's
     * integration test instead, since it needs a real Table/GinReadStrategy to search.
     */
    class PgFdwGinQualRoutingTest : public ::testing::Test {
    protected:
        ConstQual *make_qual(AttrNumber varattno, QualOpName op, Oid typeoid, Datum value)
        {
            ConstQual *q = new ConstQual();
            q->base.varattno = varattno;
            q->base.op = op;
            q->base.typeoid = typeoid;
            q->base.right_type = T_Const;
            q->base.isArray = false;
            q->base.useOr = false;
            q->value = value;
            q->isnull = false;
            quals_owned.push_back(q);
            return q;
        }

        void TearDown() override
        {
            for (auto *q : quals_owned) {
                delete q;
            }
        }

        std::vector<ConstQual *> quals_owned;
    };

    TEST_F(PgFdwGinQualRoutingTest, IsTypeSortable_GinSimilarNeverSortable)
    {
        // GIN_SIMILAR must stay out of the ordinary btree range-scan bound comparison
        // (_set_scan_iterators()'s switch) regardless of column type -- it's routed entirely
        // through _try_init_gin_scan() instead.
        EXPECT_FALSE(PgFdwMgr::_is_type_sortable(TEXTOID, GIN_SIMILAR));
    }

    TEST_F(PgFdwGinQualRoutingTest, IsTypeSortable_TextEqualityStillSortable)
    {
        // Regression guard: adding GIN_SIMILAR must not disturb TEXT's existing EQUALS/
        // NOT_EQUALS eligibility.
        EXPECT_TRUE(PgFdwMgr::_is_type_sortable(TEXTOID, EQUALS));
        EXPECT_TRUE(PgFdwMgr::_is_type_sortable(TEXTOID, NOT_EQUALS));
    }

    TEST_F(PgFdwGinQualRoutingTest, GetIndexQuals_SkipsGinIndexEvenForEqualsQual)
    {
        // A GIN index's own column is never a valid match for _get_index_quals() (the ordinary
        // btree-shaped prefix-match used by _compute_planning_metadata()/_init_quals()'s best-
        // index selection), even for an operator (EQUALS) that would otherwise match -- GIN
        // indexes aren't reachable through Table::index()/MergeTable's btree-shaped
        // lower_bound()/upper_bound() at all. Without this guard, a plain "WHERE tags = ..."
        // query against a GIN-indexed column would crash (Table::index() doing .at() on a
        // index id that only lives in Table's separate _gin_indexes map).
        std::map<uint32_t, SchemaColumn> columns;
        columns.try_emplace(1, SchemaColumn("tags", 1, SchemaType::TEXT, TEXTOID, true));

        springtail::Index idx;
        idx.id = 42;
        idx.access_method = IndexAccessMethod::GIN;
        idx.columns.push_back(springtail::Index::Column{0, 1, "gin_trgm_ops"});

        List *qual_list = nullptr;
        qual_list = lappend(
            qual_list, make_qual(1, EQUALS, TEXTOID, PointerGetDatum(cstring_to_text("apple"))));

        auto matched = PgFdwMgr::_get_index_quals(columns, idx, qual_list);
        EXPECT_TRUE(matched.empty());
    }

    TEST_F(PgFdwGinQualRoutingTest, GetIndexQuals_StillMatchesOrdinaryBtreeIndex)
    {
        // Regression guard: the access_method == BTREE case (the overwhelming majority of
        // indexes) must still match exactly as before.
        std::map<uint32_t, SchemaColumn> columns;
        columns.try_emplace(1, SchemaColumn("tags", 1, SchemaType::TEXT, TEXTOID, true));

        springtail::Index idx;
        idx.id = 42;
        idx.access_method = IndexAccessMethod::BTREE;
        idx.columns.push_back(springtail::Index::Column{0, 1, ""});

        List *qual_list = nullptr;
        qual_list = lappend(
            qual_list, make_qual(1, EQUALS, TEXTOID, PointerGetDatum(cstring_to_text("apple"))));

        auto matched = PgFdwMgr::_get_index_quals(columns, idx, qual_list);
        ASSERT_EQ(matched.size(), 1u);
        EXPECT_EQ(matched[0]->base.op, EQUALS);
    }
}
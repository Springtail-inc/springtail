#include <benchmark/benchmark.h>

#include <fmt/format.h>

#include <common/init.hh>
#include <common/environment.hh>
#include <common/threaded_test.hh>

#include <storage/btree.hh>
#include <storage/mutable_btree.hh>
#include "common/constants.hh"

using namespace springtail;

namespace {
class BenchmarkHelper {
    public:
    void SetUp() {
        _base_dir = std::filesystem::temp_directory_path() / "benchmark_btree";
        std::filesystem::create_directories(_base_dir);

        // construct a schema for testing
        std::vector<SchemaColumn> columns({
                { "table_id", 0, SchemaType::UINT64, 0, false, 1},
                { "name", 1, SchemaType::TEXT, 0, false, 0 },
                { "offset", 2, SchemaType::UINT64, 0, false },
                { "index", 3, SchemaType::UINT16, 0, false, 2 }
            });
        _schema = std::make_shared<ExtentSchema>(columns);
        _keys = std::vector<std::string>({"name", "table_id", "index"});
    }

    void TearDown() {
        std::filesystem::remove_all(_base_dir);
    }

    void populate_btree_many_rows(MutableBTreePtr btree, int row_count) {
        auto key_fields = std::make_shared<FieldArray>(3);
        auto value_fields = std::make_shared<FieldArray>(1);
        for (int i = 0; i < row_count; i++) {
            (*key_fields)[0] = std::make_shared<ConstTypeField<uint64_t>>(i);
            (*key_fields)[1] = std::make_shared<ConstTypeField<std::string>>(fmt::format("name_{}", i));
            (*key_fields)[2] = std::make_shared<ConstTypeField<uint64_t>>(i % 1000);
            (*value_fields)[0] = std::make_shared<ConstTypeField<uint16_t>>(i % 1000);

            auto kvt = std::make_shared<KeyValueTuple>(key_fields, value_fields, nullptr);
            btree->insert(kvt);
        }
    }

    ExtentSchemaPtr _schema;
    std::vector<std::string> _keys;

    std::filesystem::path _base_dir;

    std::shared_ptr<MutableBTree>
    create_mutable_btree(const std::filesystem::path &name,
                         uint64_t xid)
    {
        // construct a mutable b-tree for inserting data
        auto btree = std::make_shared<MutableBTree>(name, _keys, _schema, xid, constant::MAX_EXTENT_SIZE);

        btree->init_empty();
        return btree;
    }
};

}

static void BM_BTreeLookupHelper(benchmark::State& state, bool miss) {
    BenchmarkHelper helper;
    helper.SetUp();
    auto path = helper._base_dir / "BenchmarkInsert";
    auto btree = helper.create_mutable_btree(path, 1);
    int row_count = state.range(0);
    helper.populate_btree_many_rows(btree, row_count);
    auto offset = btree->finalize();

    // get a pointer to the read-only btree
    auto tree = std::make_shared<BTree>(path, 1, helper._schema, offset, constant::MAX_EXTENT_SIZE);
    std::mt19937 generator{std::random_device{}()};
    std::uniform_int_distribution<> dist(0, row_count - 1);

    for (auto _ : state) {
        auto key_fields = std::make_shared<FieldArray>(3);
        int64_t to_lookup = dist(generator);
        key_fields->at(0) = std::make_shared<ConstTypeField<std::string>>(fmt::format("name_{}", to_lookup));
        key_fields->at(1) = std::make_shared<ConstTypeField<uint64_t>>(to_lookup);
        key_fields->at(2) = std::make_shared<ConstTypeField<uint16_t>>((to_lookup % 1000) + miss);
        auto key_tuple = std::make_shared<FieldTuple>(key_fields, nullptr);

        auto it = tree->lower_bound(key_tuple);
        if (miss && to_lookup == row_count - 1) {
            CHECK(it == tree->end());
        } else {
            CHECK(it != tree->end());
        }
    }

    // Cleanup
    helper.TearDown();
}

static void BM_BTreeLookupMiss(benchmark::State& state) {
    BM_BTreeLookupHelper(state, true);
}

static void BM_BTreeLookupHit(benchmark::State& state) {
    BM_BTreeLookupHelper(state, false);
}

BENCHMARK(BM_BTreeLookupMiss)
    ->Arg(10)
    ->Arg(1000)
    ->Arg(100000)
    ->UseRealTime();

BENCHMARK(BM_BTreeLookupHit)
    ->Arg(10)
    ->Arg(1000)
    ->Arg(100000)
    ->UseRealTime();

static void BM_BTreeInsert(benchmark::State& state) {
    BenchmarkHelper helper;
    helper.SetUp();
    auto path = helper._base_dir / "BenchmarkInsert";
    int row_count = state.range(0);

    for (auto _ : state) {
        auto btree = helper.create_mutable_btree(path, 1);
        helper.populate_btree_many_rows(btree, row_count);
        btree->finalize();
    }

    state.SetItemsProcessed(state.iterations() * row_count);

    // Cleanup
    helper.TearDown();
}
BENCHMARK(BM_BTreeInsert)
    ->Arg(1000)
    ->Arg(10000)
    ->Arg(100000)
    ->UseRealTime();


int main(int argc, char **argv) {
    benchmark::Initialize(&argc, argv);

    springtail_init(false, std::nullopt, LOG_NONE);

    benchmark::RunSpecifiedBenchmarks();

    springtail_shutdown();
    return 0;
}

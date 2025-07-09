#include <benchmark/benchmark.h>

#include <common/init.hh>
#include <storage/extent.hh>
#include <storage/field.hh>

using namespace springtail;

namespace {
class Parent {
public:
    virtual int32_t get_int32(const void *row) const {
        throw 1;
    }
};

class MParent : public Parent {
public:
    virtual void set_int32(void *row, int32_t val) {
        throw 1;
    }
};

class BogusField : public MParent {
public:
    int32_t get_int32(const void *row) const override {
        return 0;
    }
};

class BogusField2 : public MParent {
public:
    int32_t get_int32(const void *row) const override {
        return 0;
    }
};

class Int32Field : public MParent {
public:
    Int32Field(SchemaType type, uint32_t offset)
        : _type(type),
          _can_null(false),
          _can_undefined(false),
          _offset(offset)
    { }

    SchemaType get_type() const {
        return _type;
    }

    void set_int32(void *row, int32_t val) override {
        *reinterpret_cast<int32_t *>(reinterpret_cast<Extent::Row *>(row)->data() + _offset) = val;
    }

    int32_t get_int32(const void *row) const override {
        auto e_row = reinterpret_cast<const Extent::Row *>(row);
        return *reinterpret_cast<const int32_t *>(e_row->data() + _offset);
    }

private:
    SchemaType _type;

    bool _can_null;
    bool _can_undefined;

    uint32_t _offset;
    uint8_t _bool_bitmask;

    uint32_t _null_offset;
    uint8_t _null_bitmask;

    uint32_t _undefined_offset;
    uint8_t _undefined_bitmask;
};

}

static void BM_RawAccess(benchmark::State &state) {
    std::vector<std::pair<int, int>> raw_values(state.range());
    for (int i = 0; i < state.range(); ++i) {
        raw_values[i].first = i + 1;
        raw_values[i].second = -(i + 1);
    }
    uint64_t expected = (state.range() * (state.range() + 1)) / 2;

    uint64_t result = 0;
    for (auto _ : state) {
        for (auto value : raw_values) {
            result += value.first;
        }
    }

    CHECK_EQ(result, expected * state.iterations());
}
BENCHMARK(BM_RawAccess)->Arg(4096)->MinTime(1.0)->UseRealTime();

static void BM_FieldAccess(benchmark::State &state) {
    std::vector<uint8_t> types;
    types.push_back(static_cast<uint8_t>(SchemaType::INT32));
    types.push_back(static_cast<uint8_t>(SchemaType::INT32));

    // construct the extent values
    auto extent = std::make_shared<Extent>(ExtentType(), 0, 8, types);
    std::vector<MutableFieldPtr> fields;
    fields.push_back(std::make_shared<ExtentField>(SchemaType::INT32, 0));
    fields.push_back(std::make_shared<ExtentField>(SchemaType::INT32, 4));
    for (int i = 1; i <= state.range(); ++i) {
        auto row = extent->append();
        fields[0]->set_int32(&row, i);
        fields[1]->set_int32(&row, -i);
    }
    uint64_t expected = (state.range() * (state.range() + 1)) / 2;

    uint64_t result = 0;
    auto field = fields[0];
    for (auto _ : state) {
        for (auto row : *extent) {
            result += field->get_int32(&row);
        }
    }

    CHECK_EQ(result, expected * state.iterations());
}
BENCHMARK(BM_FieldAccess)->Arg(4096)->MinTime(1.0)->UseRealTime();

static void BM_NewFieldAccess(benchmark::State &state) {
    std::vector<uint8_t> types;
    types.push_back(static_cast<uint8_t>(SchemaType::INT32));
    types.push_back(static_cast<uint8_t>(SchemaType::INT32));

    // construct the extent values
    auto extent = std::make_shared<Extent>(ExtentType(), 0, 8, types);
    std::vector<std::shared_ptr<MParent>> fields;
    fields.push_back(std::make_shared<Int32Field>(SchemaType::INT32, 0));
    fields.push_back(std::make_shared<Int32Field>(SchemaType::INT32, 4));
    for (int i = 1; i <= state.range(); ++i) {
        auto row = extent->append();
        fields[0]->set_int32(&row, i);
        fields[1]->set_int32(&row, -i);
    }
    uint64_t expected = (state.range() * (state.range() + 1)) / 2;

    uint64_t result = 0;
    auto field = fields[0];
    for (auto _ : state) {
        for (auto row : *extent) {
            result += field->get_int32(&row);
        }
    }

    CHECK_EQ(result, expected * state.iterations());
}
BENCHMARK(BM_NewFieldAccess)->Arg(4096)->MinTime(1.0)->UseRealTime();

int
main(int argc,
     char **argv)
{
    benchmark::Initialize(&argc, argv);
    springtail_init(false, std::nullopt, LOG_NONE);

    benchmark::RunSpecifiedBenchmarks();

    springtail_shutdown();
    return 0;
}

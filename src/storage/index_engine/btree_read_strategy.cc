#include <algorithm>

#include <storage/index_engine/btree_read_strategy.hh>

namespace springtail {
    Extent::Iterator
    BtreeReadStrategy::lower_bound(std::shared_ptr<Extent> extent,
                                   ExtentSchemaPtr schema,
                                   TuplePtr key) const
    {
        return std::ranges::lower_bound(*extent, *key,
                                        [](const Tuple &lhs, const Tuple &rhs) {
                                            return lhs.less_than(rhs);
                                        },
                                        [&schema](const Extent::Row &row) {
                                            return FieldTuple(schema->get_sort_fields(), &row);
                                        });
    }

    Extent::Iterator
    BtreeReadStrategy::upper_bound(std::shared_ptr<Extent> extent,
                                   ExtentSchemaPtr schema,
                                   TuplePtr key) const
    {
        return std::ranges::upper_bound(*extent, *key,
                                        [](const Tuple &lhs, const Tuple &rhs) {
                                            return lhs.less_than(rhs);
                                        },
                                        [&schema](const Extent::Row &row) {
                                            return FieldTuple(schema->get_sort_fields(), &row);
                                        });
    }
}

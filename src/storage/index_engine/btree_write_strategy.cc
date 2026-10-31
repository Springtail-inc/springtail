#include <algorithm>

#include <storage/index_engine/btree_write_strategy.hh>

namespace springtail {
    Extent::Iterator
    BtreeWriteStrategy::choose_child(std::shared_ptr<Extent> extent,
                                     ExtentSchemaPtr schema,
                                     TuplePtr key) const
    {
        auto i = std::ranges::lower_bound(*extent, *key,
                                          [](const Tuple &lhs, const Tuple &rhs) {
                                              return lhs.less_than(rhs);
                                          },
                                          [&schema](const Extent::Row &row) {
                                              return FieldTuple(schema->get_sort_fields(), &row);
                                          });

        if (i == extent->end()) {
            // key is greater than every entry -- follow the last child, mirroring
            // MutableBTree::_find_leaf()'s behavior.
            i = extent->last();
        }

        return i;
    }
}

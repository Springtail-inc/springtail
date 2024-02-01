#include <storage/field.hh>

namespace springtail {

    TuplePtr
    ReadFieldArray::bind(const Extent::Row &row)
    {
        return std::make_shared<RowTuple>(shared_from_this(), row);
    }

    TuplePtr
    MutableFieldArray::bind(const Extent::Row &row)
    {
        return std::make_shared<RowTuple>(shared_from_this(), row);
    }

    MutableTuplePtr
    MutableFieldArray::bind(const Extent::MutableRow &row)
    {
        return std::make_shared<MutableRowTuple>(shared_from_this(), row);
    }

}

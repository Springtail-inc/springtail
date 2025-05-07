// #include <common/logging.hh>
#include <xxhash.h>

#include <storage/compressors.hh>
#include <storage/extent.hh>
#include <storage/field.hh>

namespace springtail {

    std::pair<ExtentPtr, ExtentPtr>
    Extent::split(ExtentSchemaPtr schema)
    {
        // determine a half-way point
        uint32_t half = row_count() / 2;

        // a split extent can no longer be a root
        ExtentHeader header = _header;
        header.type = ExtentType(header.type.is_branch());

        // create two empty extents
        ExtentPtr first = std::make_shared<Extent>(header);
        ExtentPtr second = std::make_shared<Extent>(header);

        /* XXX it would be more efficient to direct-copy the fixed data and then check the
           variable-sized data pointers to determine which ones need to be copied over, but
           this was easier to implement. */

        // copy each row by copying it into the appropriate extent
        auto mutable_array = schema->get_mutable_fields();
        auto array = schema->get_fields();

        // copy the rows from this extent to the first half extent
        for (auto i = 0; i < half; i++) {
            Row &&insert_row = first->append();

            MutableTuple tuple(mutable_array, &insert_row);
            tuple.assign(FieldTuple(array, &*(this->at(i))));
        }

        // copy the remaining rows from this extent to the first half extent
        for (auto i = half; i < row_count(); i++) {
            Row &&insert_row = second->append();

            MutableTuple tuple(mutable_array, &insert_row);
            tuple.assign(FieldTuple(array, &*(this->at(i))));
        }

        return std::pair<ExtentPtr, ExtentPtr>(first, second);
    }

}

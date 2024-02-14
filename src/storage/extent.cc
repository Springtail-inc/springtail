// #include <common/logging.hh>
#include <xxhash.h>

#include <storage/compressors.hh>
#include <storage/extent.hh>
#include <storage/field.hh>

namespace springtail {

    std::pair<ExtentPtr, ExtentPtr>
    Extent::split() const
    {
        // determine a half-way point
        uint32_t half = row_count() / 2;

        // create two empty extents
        ExtentPtr first = std::make_shared<Extent>(_schema, _header.type, _header.xid);
        ExtentPtr second = std::make_shared<Extent>(_schema, _header.type, _header.xid);

        /* XXX it would be more efficient to direct-copy the fixed data and then check the
           variable-sized data pointers to determine which ones need to be copied over, but
           this was easier to implement. */

        // copy each row by copying it into the appropriate extent
        auto array = _schema->get_mutable_fields();

        // copy the rows from this extent to the first half extent
        for (auto i = 0; i < half; i++) {
            MutableRow &&insert_row = first->append();
            array->bind(insert_row)->assign(array->bind(this->at(i)));
        }

        // copy the remaining rows from this extent to the first half extent
        for (auto i = half; i < row_count(); i++) {
            MutableRow &&insert_row = second->append();
            array->bind(insert_row)->assign(array->bind(this->at(i)));
        }

        return std::pair<ExtentPtr, ExtentPtr>(first, second);
    }

}

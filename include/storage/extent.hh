#pragma once

#include <cstdint>
#include <memory>
#include <vector>
#include <cassert>
#include <future>
#include <span>

#include <xxhash.h>

#include <pg_repl/pg_types.hh>

#include <storage/compressors.hh>
#include <storage/io.hh>
#include <storage/schema.hh>
#include <storage/variable_data.hh>

namespace springtail {
    // pre-declare classes to avoid circular dependencies
    class Field;
    class MutableFieldTuple;
    class CompressedExtent;

    class ExtentType {
    private:
        uint8_t _type;

        static const uint8_t LEAF = 0x00;
        static const uint8_t BRANCH = 0x01;
        static const uint8_t ROOT_MASK = 0x02;

    public:
        ExtentType()
            : _type(LEAF)
        { }

        explicit ExtentType(uint8_t type)
            : _type(type)
        { }

        explicit ExtentType(bool branch, bool root=false)
            : _type(((branch) ? BRANCH : LEAF) | ((root) ? ROOT_MASK : 0))
        { }

        explicit operator uint8_t() const {
            return _type;
        }

        bool is_root() const {
            return (_type & ROOT_MASK);
        }

        bool is_leaf() const {
            return (_type & ~ROOT_MASK) == LEAF;
        }

        bool is_branch() const {
            return (_type & ~ROOT_MASK) == BRANCH;
        }
    };

    /** Definition of the extent header. */
    struct ExtentHeader {
    public:
        ExtentType type; ///< The type of the extent.
        uint64_t xid; ///< The XID that this extent is valid from.
        uint32_t row_size; ///< The size of a row in the extent.

        /** The field types contained within each row.  The top bit of each byte indicates nullable,
            the bottom 7 bits indicate the type. */
        std::vector<uint8_t> field_types;

        uint64_t prev_offset; ///< The location of the previous extent that this extent is overwriting.  Set to UNKNOWN_EXTENT for new extents.

        /** Constructor for uncommitted extents.*/
        ExtentHeader(ExtentType type,
                     uint64_t xid,
                     uint32_t row_size,
                     const std::vector<uint8_t> &types,
                     uint64_t prev_offset = constant::UNKNOWN_EXTENT)
            : type(type),
              xid(xid),
              row_size(row_size),
              field_types(types),
              prev_offset(prev_offset)
        { }

        /** Constructor that deserializes the header. */
        ExtentHeader(std::shared_ptr<std::vector<char>> data)
        {
            uint32_t field_count;

            std::memcpy(&xid, data->data(), sizeof(uint64_t));
            std::memcpy(&prev_offset, data->data() + 8, sizeof(uint64_t));
            std::memcpy(&row_size, data->data() + 16, sizeof(uint32_t));
            std::memcpy(&field_count, data->data() + 20, sizeof(uint32_t));
            field_types.resize(field_count);
            std::memcpy(field_types.data(), data->data() + 24, field_count);
            type = ExtentType(*reinterpret_cast<uint8_t *>(data->data() + 24 + field_count));
        }

        /** Serialize the header. */
        std::vector<char> pack() const
        {
            std::vector<char> data(25 + field_types.size());
            uint32_t field_count = field_types.size();

            std::memcpy(data.data(), &xid, sizeof(uint64_t));
            std::memcpy(data.data() + 8, &prev_offset, sizeof(uint64_t));
            std::memcpy(data.data() + 16, &row_size, sizeof(uint32_t));
            std::memcpy(data.data() + 20, &field_count, sizeof(uint32_t));
            std::memcpy(data.data() + 24, field_types.data(), field_count);
            *reinterpret_cast<uint8_t *>(data.data() + 24 + field_count) =
                static_cast<uint8_t>(type);

            return data;
        }
    };

    /** Provides the interface for a single extent within a datafile.
     *
     *  An extent is composed of a header block, the fixed data, and the variable data.
     *
     *  The header block contains the schema ID and commit ID at which this extent was written.
     *
     *  The fixed data contains fixed-width rows of data defined by the schema.  By using
     *  fixed-width rows, it becomes possible to cheaply perform array-like operations against the
     *  rows in an extent such as index-based access or binary search.  All columns are represented
     *  by a fixed position within the fixed-width row to enable efficient scanning of data.
     *
     *  The variable data contains any variable-length values for columns such as text or binary
     *  data.  The extent does internal de-duplication of variable-length data to reduce space
     *  utilization.  The fixed data contains pointers into the variable data for any such columns.
     */
    class Extent {
    public:
        // forward declaration
        class Iterator;

        /** Interface to access a row in an extent. */
        class Row {
        public:
            Row() : extent(nullptr), offset(0) {}
            Row(Extent *e, uint32_t o) : extent(e), offset(o) {}

            /** Retrieve text from the variable data. */
            std::string_view get_text(uint32_t offset) const {
                return extent->get_text(offset);
            }

            /** Store text into the variable data and return the offset. */
            uint32_t set_text(const std::string_view &value) {
                return extent->add_variable(value.data(), value.size());
            }

            /** Retrieve binary data from the variable data. */
            const std::span<const char> get_binary(uint32_t offset) const {
                return extent->get_binary(offset);
            }

            /** Store binary data into the variable data and return the offset. */
            uint32_t set_binary(const std::span<const char> &value) {
                return extent->add_variable(value.data(), value.size());
            }

            char *data() const {
                return extent->_fixed_data->data() + offset;
            }

            bool operator==(const Row &rhs) const {
                return (extent == rhs.extent && offset == rhs.offset);
            }

        private:
            // grant Iterator access to the Row internals
            friend Iterator;
            friend Extent;

            Extent * extent;
            uint32_t offset;
        };

        /** Iterator over the rows in an extent. */
        class Iterator {
            // to allow use of the private constructor
            friend Extent;

        private:
            Row _row;

            Iterator(Extent *extent, uint32_t offset)
                : _row(extent, offset)
            { }

        public:
            Iterator() : _row(nullptr, 0) {}

            using iterator_category = std::random_access_iterator_tag;
            using difference_type   = std::ptrdiff_t;
            using value_type        = const Row;
            using pointer           = const Row *;  // or also value_type*
            using reference         = const Row &;  // or also value_type&

            reference operator*() const { return _row; }
            pointer operator->() const { return &_row; }
            Iterator& operator++() {
                assert(_row.offset + _row.extent->row_size() <= _row.extent->row_count() * _row.extent->row_size());
                _row.offset += _row.extent->row_size();
                return *this;
            }
            Iterator operator++(int) { Iterator tmp = *this; ++(*this); return tmp; }
            Iterator& operator--() { _row.offset -= _row.extent->row_size(); return *this; }
            Iterator operator--(int) { Iterator tmp = *this; --(*this); return tmp; }
            friend bool operator==(const Iterator& a, const Iterator& b) { return a._row.offset == b._row.offset; }
            friend bool operator!= (const Iterator& a, const Iterator& b) { return a._row.offset != b._row.offset; }

            Iterator &operator+=(difference_type n) { _row.offset += _row.extent->row_size() * n; return *this; }
            Iterator &operator-=(difference_type n) { _row.offset -= _row.extent->row_size() * n; return *this; }

            Iterator operator+(difference_type n) const { Iterator tmp = *this; tmp += n; return tmp; }
            friend Iterator operator+(difference_type n, const Iterator &i) { return i + n; }
            Iterator operator-(difference_type n) const { Iterator tmp = *this; tmp -= n; return tmp; }
            friend Iterator operator-(difference_type n, const Iterator &i) { return i - n; }
            friend difference_type operator-(const Iterator &a, const Iterator &b) {
                return (a._row.offset - b._row.offset) / a._row.extent->row_size();
            }
        };

        /** Returns an iterator to the first row of the extent. */
        Iterator begin() {
            return Iterator(this, 0);
        }

        /** Returns an iterator to the last row of the extent. */
        Iterator last() {
            return Iterator(this, _fixed_data->size() - _header.row_size);
        }

        /** Returns an iterator that matches the end of the extent. */
        Iterator end() {
            return Iterator(this, _fixed_data->size());
        }

    private:
        // The underlying raw data of this extent.
        ExtentHeader _header; ///< Header data for the extent.
        std::shared_ptr<std::vector<char>> _fixed_data; ///< Storage for the fixed column data.
        std::shared_ptr<VariableData> _variable_data;

        /** Hash table for the variable data, used for duplicate detection. */
        std::unordered_map<std::string_view, uint32_t> _variable_hash;

        void _populate_vhash() {
            _variable_data->populate_hash(_variable_hash);
        }

    public:
        Extent(ExtentType type,
               uint64_t xid,
               uint32_t row_size,
               std::vector<uint8_t> types)
            : _header(type, xid, row_size, types, constant::UNKNOWN_EXTENT)
        {
            // empty extent
            _fixed_data = std::make_shared<std::vector<char>>();
            _variable_data = std::make_shared<VariableData>();
        }

        explicit Extent(const std::vector<std::shared_ptr<std::vector<char>>> &data)
            : _header(data[0]),
              _fixed_data(data[1]),
              _variable_data(std::make_shared<VariableData>(std::move(*data[2])))
        {
            _populate_vhash();
        }

        explicit Extent(const ExtentHeader &header)
            : _header(header)
        {
            // empty extent
            _fixed_data = std::make_shared<std::vector<char>>();
            _variable_data = std::make_shared<VariableData>();
        }

        Extent(const Extent &extent)
            : _header(extent._header)
        {
            // copy the data
            _fixed_data = std::make_shared<std::vector<char>>(*extent._fixed_data);
            _variable_data = std::make_shared<VariableData>(*extent._variable_data);

            _populate_vhash();
        }

        Extent(Extent &&other)
            : _header(other._header),
              _fixed_data(std::move(other._fixed_data)),
              _variable_data(std::move(other._variable_data))
        {
            other._fixed_data.reset();
            _populate_vhash();
        }

        Extent() = delete;
        Extent& operator=(const Extent&) = delete;

        ExtentHeader &header() {
            return _header;
        }

        ExtentType type() const {
            return _header.type;
        }

        bool empty() const {
            return _fixed_data->empty();
        }

        uint32_t row_size() const {
            return _header.row_size;
        }

        uint32_t byte_count() const {
            return _fixed_data->size() + _variable_data->size();
        }

        uint32_t row_count() const {
            return _fixed_data->size() / _header.row_size;
        }

        /** Return the last row in the extent. */
        Row back() {
            return Row(this, _fixed_data->size() - _header.row_size);
        }

        /** Find an existing row in the extent. */
        Iterator at(uint32_t index) {
            assert(index * _header.row_size < _fixed_data->size());
            return Iterator(this, index * _header.row_size);
        }

        /** Allocates space for a row to the end of the extent and returns an accessor to set the data in the row. */
        Row append() {
            uint32_t offset = _fixed_data->size();
            _fixed_data->resize(offset + _header.row_size);

            return Row(this, offset);
        }

        /** Allocates space for a new row at a specific existing position in the extent, shifting
            other rows further in the extent.
            Note: Unstable Interface */
        Row insert(const Iterator &pos) {
            // if the position is the end of the extent, just append()
            if (pos == end()) {
                return append();
            }

            // resize the data for the new row
            _fixed_data->resize(_fixed_data->size() + _header.row_size);

            assert(pos->offset < _fixed_data->size());

            // shift the existing data
            std::copy_backward(_fixed_data->data() + pos->offset,
                               _fixed_data->data() + _fixed_data->size() - _header.row_size,
                               _fixed_data->data() + _fixed_data->size());
            std::fill(_fixed_data->data() + pos->offset,
                      _fixed_data->data() + pos->offset + _header.row_size, char(0));

            return Row(this, pos->offset);
        }

        /**
         * Removes one or more rows from the extent.
         */
        void remove(const Iterator &pos, int count = 1) {
            if (pos == end()) {
                return;
            }

            // XXX how to clean up variable data from the row?  need some kind of reference counting

            // shift the existing data forward to the current position to remove the row
            const char *start = _fixed_data->data() + pos->offset + (_header.row_size * count);
            const char *end = _fixed_data->data() + _fixed_data->size();
            char *target = _fixed_data->data() + pos->offset;
            if (start < end) {
                // copy the remaining data
                std::copy(start, end, target);

                // resize the data to match the updated size
                _fixed_data->resize(_fixed_data->size() - (_header.row_size * count));
            } else {
                // removed all of the rows past the iterator
                _fixed_data->resize(pos->offset);
            }
        }

        /** Retrieve text from the variable data at a given offset. */
        std::string_view get_text(uint32_t offset) const {
            auto data = _variable_data->data(offset);

            // first 4 bytes are the length of the string
            uint32_t size;
            std::memcpy(&size, data, 4);

            // remainder is the string
            return std::string_view(reinterpret_cast<const char *>(data + 4), size);
        }

        /** Retrieve binary data from the variable data at a given offset. */
        const std::span<const char> get_binary(uint32_t offset) const {
            auto data = _variable_data->data(offset);

            // first 4 bytes are the length of the data
            uint32_t size;
            std::memcpy(&size, data, 4);

            // remainder is the binary data
            return std::span<const char>(data + 4, data + 4 + size);
        }

        /** Add variable-sized data to the extent. */
        uint32_t add_variable(const char *buffer, uint32_t size)
        {
            std::string_view value(buffer, size);

            // check if the value already exists
            auto i = _variable_hash.find(value);
            if (i != _variable_hash.end()) {
                return i->second;
            }

            // if doesn't exist, append and return the new location
            auto data_pos = _variable_data->push_back(buffer, size);

            // store the new offset into the hash table
            std::string_view new_value(data_pos.data + 4, size);
            _variable_hash[new_value] = data_pos.offset;

            return data_pos.offset;
        }

        /**
         * Split the extent into two halves.  This Extent remains as the first half, the returned
         * Extent is the second half.  This should only be called if there are sufficient entries in
         * the extent to warrant a split.
         */
        std::pair<std::shared_ptr<Extent>, std::shared_ptr<Extent>> split(ExtentSchemaPtr schema);

        std::future<std::shared_ptr<IOResponseAppend>>
        async_flush(std::shared_ptr<IOHandle> handle, io_append_callback_fn callback = {})
        {
            std::shared_ptr<std::vector<char>> header = std::make_shared<std::vector<char>>(_header.pack());

            std::shared_ptr<std::vector<char>> vdata = std::make_shared<std::vector<char>>(_variable_data->size());
            _variable_data->copy_into(vdata->data());

            return handle->async_append({header, _fixed_data, vdata}, callback);
        }

        std::string serialize()
        {
            size_t size = sizeof(uint32_t) * 2 + _fixed_data->size() + _variable_data->size();
            std::string data(size, 0);

            uint32_t fsize = _fixed_data->size();
            uint32_t vsize = _variable_data->size();

            sendint32(fsize, data.data());
            std::copy_n(_fixed_data->data(), fsize, data.data() + 4);

            sendint32(vsize, data.data() + 4 + fsize);
            _variable_data->copy_into(data.data() + 4 + fsize + 4);

            return data;
        }

        void deserialize(const std::string &data)
        {
            uint32_t fsize = recvint32(data.data());
            _fixed_data->resize(fsize);
            std::copy_n(data.data() + 4, fsize, _fixed_data->data());

            uint32_t vsize = recvint32(data.data() + 4 + fsize);
            std::vector<char> vdata(vsize);
            std::memcpy(vdata.data(), data.data() + 4 + fsize + 4, vsize);
            _variable_data = std::make_shared<VariableData>(std::move(vdata));
        }
    };

    /** Pointer typedef for Extent. */
    typedef std::shared_ptr<Extent> ExtentPtr;

    /** Cache of <file, extent_id> -> Extent */
    typedef ObjectCache<std::pair<std::filesystem::path, uint64_t>, Extent> ExtentCache;
    typedef std::shared_ptr<ExtentCache> ExtentCachePtr;
}

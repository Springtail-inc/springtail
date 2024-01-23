#pragma once

#include <iostream>
#include <memory>
#include <vector>
#include <cassert>

#include <xxhash.h>

#include <storage/schema.hh>
#include <storage/compressors.hh>

namespace springtail {
    // pre-declare classes to avoid circular dependencies
    class Field;
    class CompressedExtent;

    enum class ExtentType : uint8_t {
        LEAF = 0,
        BRANCH = 1,
        ROOT = 2
    };

    /** Definition of the extent header. */
    struct ExtentHeader {
    public:
        ExtentType type; ///< The type of the extent.
        uint64_t xid; ///< The XID that this extent is valid from.
        uint64_t prev_offset; ///< The location of the previous extent that this extent is overwriting.  Set to zero for new extents.

        /** Constructor for uncommitted extents.*/
        ExtentHeader(ExtentType type, uint64_t xid, uint64_t prev = 0)
            : type(type),
              xid(xid),
              prev_offset(prev)
        { }

        /** Copy constructor. */
        ExtentHeader(const ExtentHeader &header) = default;

        /** Constructor that deserializes the header. */
        ExtentHeader(std::shared_ptr<std::vector<char>> data)
        {
            std::copy_n(data->data(), sizeof(uint8_t),
                        reinterpret_cast<char *>(&type));
            std::copy_n(data->data(), sizeof(uint64_t),
                        reinterpret_cast<char *>(&xid));
            std::copy_n(data->data(), sizeof(uint64_t),
                        reinterpret_cast<char *>(&prev_offset));
        }

        /** Serialize the header. */
        std::vector<char> pack()
        {
            std::vector<char> data(9);
            std::copy_n(reinterpret_cast<char *>(&type),
                        sizeof(uint8_t), data.data());
            std::copy_n(reinterpret_cast<char *>(&xid),
                        sizeof(uint64_t), data.data());
            std::copy_n(reinterpret_cast<char *>(&prev_offset),
                        sizeof(uint64_t), data.data());
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
        // pre-declarations
        class Row;

        /** Interface to write to a row in an extent. */
        class MutableRow {
            // grant Row access to the _extent pointer for it's converstion constructor
            friend Row;

        private:
            Extent * const _extent;

        public:
            char * const data;

            MutableRow(Extent *e, char *d)
                : _extent(e), data(d)
            { }

            virtual ~MutableRow()
            { }

            /** Store text into the variable data and return the offset. */
            uint32_t set_text(const std::string &value) {
                return _extent->add_variable(reinterpret_cast<const char *>(value.data()), value.size());
            }

            /** Store binary data into the variable data and return the offset. */
            uint32_t set_binary(const std::vector<char> &value) {
                return _extent->add_variable(value.data(), value.size());
            }

            /** Finalize the row by writing any outstanding untyped data to the extent. */
            void finalize() {
                // currently empty, could be used later for any cross-row compression techniques
            }
        };

        /** Interface to read a row in an extent. */
        class Row {
        public:
            const Extent * const extent;
            const char * data;

            Row(const Extent *e, const char *d)
                : extent(e), data(d)
            { }

            Row(const Row &r)
                : extent(r.extent), data(r.data)
            { }

            Row(const MutableRow &r)
                : extent(r._extent), data(r.data)
            { }
        };

        /** Iterator over the rows in an extent. */
        class Iterator {
            // to allow use of the private constructor
            friend Extent;

        private:
            Row _row;

            Iterator(const Extent *extent, const char *data)
                : _row(extent, data)
            { }

        public:
            Iterator(const Iterator &i)
                : _row(i._row)
            { }

            using iterator_category = std::random_access_iterator_tag;
            using difference_type   = std::ptrdiff_t;
            using value_type        = const Row;
            using pointer           = const Row *;  // or also value_type*
            using reference         = const Row &;  // or also value_type&

            reference operator*() const { return _row; }
            pointer operator->() { return &_row; }
            Iterator& operator++() { _row.data += _row.extent->row_size(); return *this; }
            Iterator operator++(int) { Iterator tmp = *this; ++(*this); return tmp; }
            friend bool operator==(const Iterator& a, const Iterator& b) { return a._row.data == b._row.data; }
            friend bool operator!= (const Iterator& a, const Iterator& b) { return a._row.data != b._row.data; }
            
            Iterator &operator+=(difference_type n) { _row.data += _row.extent->row_size() * n; return *this; }
            Iterator &operator-=(difference_type n) { _row.data -= _row.extent->row_size() * n; return *this; }
            
            Iterator operator+(difference_type n) const { Iterator tmp = *this; tmp += n; return tmp; }
            friend Iterator operator+(difference_type n, const Iterator &i) { return i + n; }
            Iterator operator-(difference_type n) const { Iterator tmp = *this; tmp -= n; return tmp; }
        };

        /** Returns an iterator to the first row of the extent. */
        Iterator begin() const {
            return Iterator(this, _fixed_data.data());
        }

        /** Returns an iterator that matches the end of the extent. */
        Iterator end() const {
            return Iterator(this, _fixed_data.data() + _fixed_data.size());
        }

    private: // types

    private:
        /** Defines the schema of the table that this extent is part of. */
        std::shared_ptr<ExtentSchema> _schema;

        // The underlying raw data of this extent.
        ExtentHeader _header; ///< Header data for the extent.
        std::shared_ptr<std::vector<char>> _fixed_data; ///< Storage for the fixed column data.
        std::shared_ptr<std::vector<char>> _variable_data; ///< Storage for the variable-sized column data, referenced by the fixed data columns.

        /** Hash table for the variable data, used for duplicate detection. */
        std::unordered_map<uint64_t, std::vector<uint32_t>> _variable_hash;

        /** The size of a row in the fixed data. */
        uint32_t _row_size;

    public:
        Extent(std::shared_ptr<Schema> schema,
               ExtentType type,
               uint64_t xid)
            : _schema(schema),
              _header(type, xid),
              _row_size(schema->row_size())
        {
            // empty extent
            _fixed_data = std::maked_shared<std::vector<char>>();
            _variable_data = std::maked_shared<std::vector<char>>();
        }

        Extent(std::shared_ptr<Schema> schema,
               uint64_t extent_id,
               const std::vector<std::shared_ptr<std::vector<char>>> &data)
            : _schema(schema),
              _header(data[0]),
              _fixed_data(data[1]),
              _variable_data(data[2]),
              _row_size(schema->row_size())
        {
            // fill the hash with the variable data
            uint32_t size;
            uint32_t offset = 0;
            while (offset < _variable_data.size()) {
                // retrieve the size
                std::copy_n(reinterpret_cast<char *>(&size), sizeof(uint32_t),
                            _variable_data->data() + offset);

                // calculate the hash
                uint64_t hash = XXH64(_variable_data->data() + offset + sizeof(uint32_t), size, 0);

                // store the hash
                _variable_hash[hash].push_back(offset);

                // move to the next entry
                offset += size + sizeof(uint32_t);
            }
        }

        ExtentHeader &header() {
            return _header;
        }

        ExtentType type() const {
            return _header.type;
        }

        std::shared_ptr<ExtentSchema> schema() const {
            return _schema;
        }

        bool empty() const {
            return _fixed_data->empty();
        }

        uint32_t row_size() const {
            return _row_size;
        }

        uint32_t byte_count() const {
            return _fixed_data->size() + _variable_data->size();
        }

        uint32_t row_count() const {
            return _fixed_data->size() / _row_size;
        }

        /** Return the last row in the extent. */
        Row back() const {
            return Row(this, _fixed_data->data() + _fixed_data.size() - _row_size);
        }

        /** Find an existing row in the extent. */
        Row at(uint32_t index) const {
            assert(index * _row_size < _fixed_data->size());
            return Row(this, _fixed_data->data() + index * _row_size);
        }

        /** Find an existing row in the extent for update. */
        MutableRow at(uint32_t index) {
            assert(index * _row_size < _fixed_data->size());
            return MutableRow(this, _fixed_data->data() + index * _row_size);
        }

        /** Allocates space for a row to the end of the extent and returns an accessor to set the data in the row. */
        MutableRow append() {
            uint32_t offset = _fixed_data->size();
            _fixed_data->resize(offset + _row_size);

            return MutableRow(this, _fixed_data->data() + offset);
        }

        /** Allocates space for a new row at a specific existing position in the extent, shifting
            other rows further in the extent.
            Note: Unstable Interface */
        MutableRow insert(uint32_t index) {
            // if the index is past the end of the extent, just append()
            if (index * _row_size >= _fixed_data->size()) {
                return append();
            }

            // resize the data for the new row
            _fixed_data->resize(_fixed_data->size() + _row_size);

            // shift the existing data
            uint32_t offset = index * _row_size;
            std::copy_backward(_fixed_data->data() + offset,
                               _fixed_data->data() + _fixed_data->size() - _row_size,
                               _fixed_data->data() + offset + _row_size);
            std::fill(_fixed_data->data() + offset,
                      _fixed_data->data() + offset + _row_size, char(0));

            return MutableRow(this, _fixed_data->data() + offset);
        }

        /**
         * Removes one or more rows from the extent.
         */
        void remove(const Iterator &pos, int count = 1) {
            if (pos == end()) {
                return;
            }

            // XXX how to clean up variable data from the row?

            // shift the existing data forward to the current position to remove the row
            const char *start = pos->_row.data + (_row_size * count);
            const char *end = _fixed_data->data() + _fixed_data->size();
            if (start < end) {
                // copy the remaining data
                std::copy(start, end, pos->_row.data);

                // resize the data to match the updated size
                _fixed_data->resize(_fixed_data->size() - (_row_size * count));
            } else {
                // removed all of the rows past the iterator
                _fixed_data->resize(pos->_row.data - _fixed_data->data());
            }
        }

        /** Retrieve text from the variable data at a given offset. */
        std::string get_text(uint32_t offset) const {
            // first 4 bytes are the length of the string
            uint32_t size;
            std::copy_n(_variable_data->data() + offset, sizeof(uint32_t),
                        reinterpret_cast<char *>(&size));

            // remainder is the string
            return std::string(reinterpret_cast<const char *>(_variable_data->data() + offset + 4), size);
        }

        /** Retrieve binary data from the variable data at a given offset. */
        std::vector<char> get_binary(uint32_t offset) const {
            // first 4 bytes are the length of the data
            uint32_t size;
            std::copy_n(_variable_data->data() + offset, sizeof(uint32_t),
                        reinterpret_cast<char *>(&size));

            // remainder is the binary data
            // XXX this performs a copy, can we avoid that?
            return std::vector<char>(_variable_data->data() + offset + 4,
                                     _variable_data->data() + offset + 4 + size);
        }

        /** Add variable-sized data to the extent. */
        uint32_t add_variable(const char *buffer, uint32_t size)
        {
            // hash the string value
            uint64_t hash = XXH64(buffer, size, 0);

            // check if the value already exists
            auto i = _variable_hash.find(hash);
            if (i != _variable_hash.end()) {
                for (uint32_t offset : i->second) {
                    // check size
                    uint32_t vsize;
                    std::copy_n(_variable_data->data() + offset, sizeof(uint32_t),
                                reinterpret_cast<char *>(&vsize));

                    if (size == vsize) {
                        // check the actual data
                        if (std::equal(_variable_data->data() + offset + 4,
                                       _variable_data->data() + offset + 4 + vsize,
                                       buffer)) {
                            // if already exists, return the existing location
                            return offset;
                        }
                    }
                }
            }

            // if doesn't exist, append and return the new location
            uint32_t new_offset = _variable_data->size();
            _variable_data->resize(_variable_data->size() + size + 4);
            std::copy_n(reinterpret_cast<char *>(&size), 4, _variable_data->data() + new_offset);
            std::copy_n(buffer, size, _variable_data->data() + new_offset + 4);

            // store the new offset into the hash table
            _variable_hash[hash].push_back(new_offset);

            return new_offset;
        }

        /**
         * Split the extent into two halves.  This Extent remains as the first half, the returned
         * Extent is the second half.  This should only be called if there are sufficient entries in
         * the extent to warrant a split.
         */
        std::pair<std::shared_ptr<Extent>, std::shared_ptr<Extent>> split(uint64_t xid=0) const
        {
            // determine a half-way point
            uint32_t half = row_count() / 2;

            // create two empty extents
            std::shared_ptr<Extent> first = std::make_shared<Extent>(_schema, _header.type, xid);
            std::shared_ptr<Extent> second = std::make_shared<Extent>(_schema, _header.type, xid);

            /* XXX it would be more efficient to direct-copy the fixed data and then check the
                   variable-sized data pointers to determine which ones need to be copied over, but
                   this was easier to implement. */

            // copy each row by copying it into the appropriate extent
            MutableFieldTuple tuple = _schema->get_mutable_fields();

            // copy the rows from this extent to the first half extent
            for (auto i = 0; i < half; i++) {
                MutableRow &&insert_row = first->append();
                tuple.copy(insert_row, tuple.bind(this->at(i)));
            }

            // copy the remaining rows from this extent to the first half extent
            for (auto i = half; i < row_count(); i++) {
                MutableRow &&insert_row = second->append();
                tuple.bind(insert_row) = tuple.bind(this->at(i));
            }
        }

        std::future<std::shared_ptr<IOResponseAppend>>
        async_flush(IOHandle &handle)
        {
            std::shared_ptr<std::vector<char>> header = std::make_shared<std::vector<char>>(_header.pack());
            return handle->async_append({header, _fixed_data, _variable_data});
        }
    };

    /** Pointer typedef for Extent. */
    typedef std::shared_ptr<Extent> ExtentPtr;
}

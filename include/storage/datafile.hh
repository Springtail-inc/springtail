#pragma once

#include <filesystem>

#include <storage/compressors.hh>
#include <storage/extent.hh>
#include <storage/io.hh>

namespace springtail {
    /** Provides the interface for a single datafile. */
    class Datafile {
    public: // constants
        /* Verification flags. */
        static const int VERIFY_NONE = 0;
        static const int VERIFY_METADATA = 1;
        static const int VERIFY_DATA = 2;

        static const int HEADER_SIZE = 37;

    private: // variables
        IOHandle _handle; ///< Handle to the file within the IO subsystem.

        // uint64_t _size; ///< The size of the file.  New extents are written to this position.
        // bool _dirty; ///< Flag indiciating if data has been written to the file since it was opened.

    private: // functions
        /** Verify all of the checksums in the datafile. */
        void _verify_checksums();

        /** Verify the header of the datafile.  Ensures that the data layout of the file matches the
            layout expected by this system. */
        void _verify_header();

    public: // iterator
        class Iterator {
            // to allow use of the private constructor
            friend Datafile;

        private:
            uint64_t _pos;
            const Datafile * const _datafile;
            std::shared_ptr<CompressedExtent> _extent;

            void _read_extent()
            {
                if (*this == _datafile.end()) {
                    _extent = nullptr;
                } else {
                    _extent = _datafile.read(_pos);
                }
            }

            Iterator(uint64_t pos, const Datafile * const datafile)
                : _pos(pos), _datafile(datafile)
            { }

        public:
            Iterator(const Iterator &i)
                : _pos(i._pos),
                  _extent(i._extent)
            { }

            using iterator_category = std::forward_iterator_tag;
            using difference_type   = std::ptrdiff_t;
            using value_type        = std::shared_ptr<CompressedExtent>;
            using pointer           = std::shared_ptr<CompressedExtent> *;  // or also value_type*
            using reference         = std::shared_ptr<CompressedExtent> &;  // or also value_type&

            reference operator*() const {
                if (_extent == nullptr) {
                    _read_extent();
                }
                return _extent;
            }
            pointer operator->() {
                if (_extent == nullptr) {
                    _read_extent();
                }
                return &_extent;
            }

            Iterator& operator++() {
                _pos += sizeof(uint32_t) + _extent->size();
                _extent = nullptr;
            }
            Iterator operator++(int) { Iterator tmp = *this; ++(*this); return tmp; }

            friend bool operator==(const Iterator& a, const Iterator& b) { return (a._pos == b._pos); }
            friend bool operator!= (const Iterator& a, const Iterator& b) { return (a._pos != b._pos); }
        };

        Iterator begin() const {
            return Iterator(HEADER_SIZE, this);
        }

        Iterator end() const {
            return Iterator(_size, this);
        }

    public: // functions
        /** Construct a datafile interface for a given path. */
        Datafile(const std::filesystem::path &filename)
        {
            IOMgr::get_instance()->open(filename);
        }

        /** Destructor.  Closes the file if open. */
        ~Datafile();

        /** Create a new datafile in the path specified at object construction. */
        void create();

        /** Opens a data file for access.  Optionally perform verification based on the provided
            verification level. */
        void open(int verify=VERIFY_NONE);

        /** Read an extent starting at the specified position. */
        std::shared_ptr<CompressedExtent> read(uint64_t pos);

        /** Append an extent to the end of the datafile.  Returns the position of the extent. */
        uint64_t append(std::shared_ptr<CompressedExtent> extent);

        /** Re-write the footer and Flush the underlying file to disk. */
        void flush();

        /** Close the file.  If the datafile is dirty then re-write the footer and flush it's contents to disk. */
        void close();
    };
}

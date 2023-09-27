#pragma once

#include <boost/filesystem.hpp>

#include <storage/compressors.hh>
#include <storage/extent.hh>

namespace st_storage {
    /** Provides the interface for a single datafile. */
    class Datafile {
    public: // constants
        /* Verification flags. */
        static const int VERIFY_NONE = 0;
        static const int VERIFY_METADATA = 1;
        static const int VERIFY_DATA = 2;

        static const int HEADER_SIZE = 37;

    private: // variables
        std::filesystem::path _filename; ///< File path for this data file.
        int _handle; ///< Handle to the actual open file.

        uint64_t _size; ///< The size of the file.  New extents are written to this position.
        bool _dirty; ///< Flag indiciating if data has been written to the file since it was opened.

    private: // functions
        /** Verify all of the checksums in the datafile. */
        void _verify_checksums();

        /** Verify the header of the datafile.  Ensures that the data layout of the file matches the
            layout expected by this system. */
        void _verify_header();

    public: // functions
        /** Construct a datafile interface for a given path. */
        Datafile(const std::filesystem::path &filename)
            : _filename(filename),
              _handle(0)
        { }

        /** Construct a datafile interface for a given path. */
        Datafile(std::filesystem::path &&filename)
            : _filename(filename),
              _handle(0)
        { }

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

#endif // __CIRCLE__DATAFILE_HH__

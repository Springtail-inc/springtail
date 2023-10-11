#include <fmt/core.h>

#include <storage/field.hh>
#include <storage/datafile.hh>

namespace springtail {

    void
    Datafile::create()
    {
        // prepare the header
        std::vector<char> header(HEADER_SIZE);

        uint8_t version = 1;
        std::copy_n(reinterpret_cast<char *>(&version), 1, header.data());

        uint32_t check_int = 0x12345678;
        std::copy_n(reinterpret_cast<char *>(&check_int), 4, header.data() + 1);

        uint64_t check_int64 = 0x123456789ABCDEF0LL;
        std::copy_n(reinterpret_cast<char *>(&check_int64), 8, header.data() + 5);

        double check_double = 3.1415926535897932384;
        std::copy_n(reinterpret_cast<char *>(&check_double), 8, header.data() + 13);

        double check_infinity = std::numeric_limits<double>::infinity();
        std::copy_n(reinterpret_cast<char *>(&check_infinity), 8, header.data() + 21);

        double check_nan = std::numeric_limits<double>::quiet_NaN();
        std::copy_n(reinterpret_cast<char *>(&check_nan), 8, header.data() + 29);
        
        // create the file
        _handle = ::open(_filename.c_str(), O_CREAT | O_RDWR);
        if (_handle < 0) {
            throw StorageError("Error creating file");
        }

        // write the header
        ::write(_handle, header.data(), HEADER_SIZE);
    }

    void
    Datafile::_verify_header()
    {
        ::lseek(_handle, 0, SEEK_SET);

        // read the whole header in one read() call
        std::vector<char> header(HEADER_SIZE);
        int error = ::read(_handle, header.data(), HEADER_SIZE);
        if (error < 0) {
            throw StorageError("Error reading file");
        }
        if (error < HEADER_SIZE) {
            throw ValidationError("Unable to read the full file header");
        }

        // verify the version
        uint8_t version;
        std::copy_n(header.data(), 1, reinterpret_cast<char *>(&version));
        if (version != 1) {
            throw ValidationError(fmt::format("Unsupported version {}", version));
        }

        // verify the endianness
        uint32_t check_int;
        std::copy_n(header.data() + 1, 4, reinterpret_cast<char *>(&check_int));
        if (check_int != 0x12345678) {
            if (check_int == 0x87654321) {
                throw ValidationError("32-bit endian mismatch");
            } else {
                throw ValidationError(fmt::format("Invalid value in endian check {}", check_int));
            }
        }

        // verify 64-bit
        uint64_t check_int64;
        std::copy_n(header.data() + 5, 8, reinterpret_cast<char *>(&check_int64));
        if (check_int64 != 0x123456789ABCDEF0LL) {
            throw ValidationError("64-bit value mismatch");
        }

        // verify double layout
        double check_double;
        std::copy_n(header.data() + 13, 8, reinterpret_cast<char *>(&check_double));
        if (fabs(3.1415926535897932384 - check_double) < 1e-18) {
            throw ValidationError("double value mismatch");
        }

        // verify double infinity
        double check_infinity;
        std::copy_n(header.data() + 21, 8, reinterpret_cast<char *>(&check_infinity));
        if (check_infinity == std::numeric_limits<double>::infinity()) {
            throw ValidationError("infinity value mismatch");
        }

        // verify double NaN
        double check_nan;
        std::copy_n(header.data() + 29, 8, reinterpret_cast<char *>(&check_nan));
        if (check_nan == std::numeric_limits<double>::quiet_NaN()) {
            throw ValidationError("NaN value mismatch");
        }
    }

    void
    Datafile::_verify_checksums()
    {
        // XXX read each extent to reconstruct and verify their checksums; can use a fast hash for this like SpookyHash or CityHash
    }

    void
    Datafile::open(int verify)
    {
        // open the file
        _handle = ::open(_filename.c_str(), O_RDWR);
        if (_handle < 0) {
            throw StorageError("Error opening file");
        }
        // CIRCLE_LOG_TRACE << _filename << std::endl;

        // optionally verify the header
        if (verify >= VERIFY_METADATA) {
            _verify_header();
        }

        // optionally verify the data
        if (verify >= VERIFY_DATA) {
            _verify_checksums();
        }
    }

    std::shared_ptr<CompressedExtent>
    Datafile::read(uint64_t pos)
    {
        // read the compressed extent from disk
        ::lseek(_handle, pos, SEEK_SET);

        int32_t size;
        ::read(_handle, &size, sizeof(uint32_t));

        std::vector<char> buffer(size);
        ::read(_handle, buffer.data(), size);

        return std::make_shared<CompressedExtent>(buffer);
    }

    uint64_t
    Datafile::append(std::shared_ptr<CompressedExtent> extent)
    {
        // update the file size
        uint64_t pos = _size;
        uint32_t size = extent->size();
        _size += sizeof(uint32_t) + size;

        // mark the file as dirty
        _dirty = true;

        // write the extent to the file
        ::lseek(_handle, pos, SEEK_SET);
        ::write(_handle, &size, sizeof(uint32_t));
        ::write(_handle, extent->data(), size);

        // return the write position
        return pos;
    }

    void
    Datafile::flush()
    {
        if (_dirty) {
            ::fsync(_handle);
            _dirty = false;
        }
    }

    void
    Datafile::close()
    {
        // close the file and fsync data to disk
        this->flush();
        ::close(_handle);
    }

}

#include <string>
#include <memory>
#include <functional>
#include <iostream>
#include <vector>
#include <cstdio>

#include <storage/io.hh>
#include <storage/exception.hh>
#include <storage/compressors.hh>

namespace springtail {

    static const char *MODE_APPEND = "a+b";
    static const char *MODE_READ = "rb";

    static const char HDR_MAGIC[3] = { 'E', 'X', 'T'};

    IOHandle::IOHandle(std::filesystem::path path, int mode) :
        _path(path),
        _file(nullptr),
        _mode(mode),
        _is_dirty(false)
    {
        if (mode == IOMgr::IO_MODE_APPEND) {
            compressor = std::make_unique<Lz4Compressor>();
        } else if (mode == IOMgr::IO_MODE_READ) {
            decompressor = std::make_unique<Lz4Decompressor>();
        }
    }

    IOHandle::IOHandle(std::FILE *file, std::filesystem::path path, int mode) :
        _path(path),
        _file(file),
        _mode(mode),
        _is_dirty(false)
    {
        if (mode == IOMgr::IO_MODE_APPEND) {
            compressor = std::make_unique<Lz4Compressor>();
        } else if (mode == IOMgr::IO_MODE_READ) {
            decompressor = std::make_unique<Lz4Decompressor>();
        }
    }

    std::shared_ptr<std::vector<char>>
    IOHandle::read(uint64_t pos)
    {
        char hdr[12];
        uint8_t count;
        uint32_t size;

        if (std::fseek(_file, pos, SEEK_SET) != 0) {
            throw StorageError();
        }

        if (std::fread(hdr, 1, 12, _file) != 12) {
            throw StorageError();
        }

        if (hdr[0] != HDR_MAGIC[0] || hdr[1] != HDR_MAGIC[1] || hdr[2] != HDR_MAGIC[2]) {
            throw StorageError();
        }

        // vector count
        count = hdr[3];

        // total uncompressed size
        std::copy_n(&hdr[4], sizeof(uint32_t), reinterpret_cast<char *>(&size));

        // output buffer
        std::shared_ptr<std::vector<char>> data_ptr = std::make_shared<std::vector<char>>(size);

        // temp buffer
        std::vector<char> compressed_data;


        uint32_t csize;
        int offset = 0;
        for (int i = 0; i < count; i++) {
            // read compressed size
            std::fread(reinterpret_cast<char *>(&csize), 1, sizeof(uint32_t), _file);

            // resize temp buffer
            compressed_data.resize(csize);

            // read compressed data and decompress
            std::fread(compressed_data.data(), 1, csize, _file);
            offset += decompressor->decompress_raw(compressed_data, *data_ptr, offset);
        }

        return data_ptr;
    }

    uint64_t
    IOHandle::append(const char *buffer, int length)
    {
        std::vector<char> data(buffer, buffer + length);
        return append(data);
    }

    uint64_t
    IOHandle::append(const std::vector<char> &data)
    {
        if (std::fseek(_file, 0, SEEK_END) != 0) {
            throw StorageError();
        }

        uint64_t offset = std::ftell(_file);

        if (data.size() == 0) {
            return offset;
        }

        // compress data
        std::vector<char> compressed_data;
        compressor->reset_stream();
        uint32_t csize = compressor->compress_raw(data, compressed_data);
        uint32_t size = data.size();

        // write out data
        // hdr magic 'EXT'
        // number of vectors 1B
        // size of uncompressed data size 4B
        // size of compressed data size 4B
        char hdr[12];
        std::copy_n(HDR_MAGIC, 3, &hdr[0]);
        hdr[3] = 1; // number of compressed vectors
        std::copy_n(reinterpret_cast<char *>(&size), sizeof(int32_t), &hdr[4]);
        std::copy_n(reinterpret_cast<char *>(&csize), sizeof(int32_t), &hdr[8]);

        std::fwrite(hdr, 1, 12, _file);
        std::fwrite(compressed_data.data(), 1, csize, _file);

        std::fflush(_file);

        _is_dirty = true;

        return offset;
    }

    uint64_t
    IOHandle::append(std::vector<char> data[], uint8_t count)
    {
        if (std::fseek(_file, 0, SEEK_END) != 0) {
            throw StorageError();
        }

        uint64_t offset = std::ftell(_file);
        if (count == 0) {
            return offset;
        }

        // do it in two passes to avoid partial writes
        // first, compress data and compute total uncompressed size
        uint32_t size = 0;
        std::vector<char> compressed_data[count];

        compressor->reset_stream();
        for (int i=0; i < count; i++) {
            size += data[i].size();
            compressor->compress_raw(data[i], compressed_data[i]);
        }

        // write out data
        char hdr[8];
        // header and number of vectors
        std::copy_n(HDR_MAGIC, 3, &hdr[0]);
        hdr[3] = count;
        // total size of uncompressed data 4B
        std::copy_n(reinterpret_cast<char *>(&size), sizeof(int32_t), &hdr[1]);
        std::fwrite(hdr, 1, 5, _file);

        // write out compressed data
        for (int i = 0; i < count; i++) {
            uint32_t csize = compressed_data[i].size();
            std::copy_n(reinterpret_cast<char *>(&csize), sizeof(int32_t), &hdr[4]);
            std::fwrite(compressed_data[i].data(), 1, csize, _file);
        }

        // flush to kernel (this does not do a sync)
        std::fflush(_file);

        _is_dirty = true;

        return offset;
    }

    void IOHandle::sync()
    {
        if (!_is_dirty) {
            return;
        }

        if (std::fflush(_file) != 0) {
            throw StorageError();
        }

        int fd = ::fileno(_file);
        if (fd < 0) {
            throw StorageError();
        }

        ::fsync(fd);
    }

    void IOHandle::read(uint64_t pos,  read_callback_fn callback)
    {

    }

    void IOHandle::append(const char *buffer, int length, write_callback_fn callback)
    {
        uint64_t offset = 0;
        callback(offset, IOStatus::SUCCESS);
    }

    void IOHandle::append(const std::vector<char> &data, write_callback_fn callback)
    {
    }

    void IOHandle::append(const std::vector<char> data[], uint8_t count, write_callback_fn callback)
    {

    }

    void IOHandle::sync(status_callback_fn callback)
    {
        sync();
        callback(IOStatus::SUCCESS);
    }


    std::shared_ptr<IOHandle>
    IOMgr::open(std::string &path, int mode)
    {
        const char *fmode;
        if (mode == IOMgr::IO_MODE_APPEND) {
            fmode = MODE_APPEND;
        } else if (mode == IOMgr::IO_MODE_READ) {
            fmode = MODE_READ;
        }

        FILE *file = std::fopen(path.c_str(), fmode);
        if (file == nullptr) {
            throw StorageError("Error opening file");
        }

        return std::make_shared<IOHandle>(file, path, mode);
    }
};

int main(int argc, char* argv[])
{
    springtail::IOHandle io("/tmp/test", springtail::IOMgr::IO_MODE_APPEND);

    char buffer[5];

    auto cb_function = [](uint64_t offset, const springtail::IOHandle::IOStatus &status) {
        int lbn = 43;
        std::cout << "Offset: " << offset << ", status: " << status << ", lbn: " << lbn <<std::endl;
    };

    io.append(buffer, 5, cb_function);

    return 0;
}

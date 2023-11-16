#include <storage/exception.hh>
#include <storage/compressors.hh>

namespace springtail {
    Lz4Compressor::Lz4Compressor()
    {
        _lz4_stream = LZ4_createStream();
    }

    Lz4Compressor::~Lz4Compressor()
    {
        LZ4_freeStream(_lz4_stream);
    }


    void
    Lz4Compressor::reset_stream()
    {
        LZ4_resetStream_fast(_lz4_stream);
    }


    uint32_t
    Lz4Compressor::compress_raw(std::shared_ptr<std::vector<char>> src, std::vector<char> &dst)
    {
        // determine the max compression size
        int target_size = LZ4_compressBound(src->size());

        dst.resize(target_size);

        // compress the data
        // NOTE: be careful with this as it requires the last 64KB of data from the stream
        // to still be in memory and accessible...
        int32_t dst_size = LZ4_compress_fast_continue(_lz4_stream,
                                                      src->data(), dst.data(),
                                                      src->size(), target_size, 1);
        if (dst_size <= 0) {
            throw ValidationError("Error compressing data");
        }

        // trim excess
        dst.resize(dst_size);

        return dst_size;
    }


    uint32_t
    Lz4Compressor::compress_block(const std::vector<char> &src, std::vector<char> &dst)
    {
        int offset = dst.size();

        // if there's no data to compress, store a zero to indicate an empty block and return
        if (src.size() == 0) {
            uint32_t dst_size = 0;
            dst.resize(offset + 4);
            std::copy_n(reinterpret_cast<char *>(&dst_size), sizeof(uint32_t), dst.data() + offset);
            return 4;
        }

        // determine the max compression size
        int target_size = LZ4_compressBound(src.size());

        // resize the buffer to fit the compressed data
        dst.resize(offset + 8 + target_size);

        // compress the data
        // NOTE: be careful with this as it requires the last 64KB of data from the stream
        // to still be in memory and accessible...
        int32_t dst_size = LZ4_compress_fast_continue(_lz4_stream,
                                                      src.data(), dst.data() + offset + 8,
                                                      src.size(), target_size, 1);
        if (dst_size <= 0) {
            throw ValidationError("Error compressing data");
        }

        // save the original data size and compressed data size
        uint32_t src_size = src.size();
        std::copy_n(reinterpret_cast<char *>(&dst_size), sizeof(int32_t), dst.data() + offset);
        std::copy_n(reinterpret_cast<char *>(&src_size), sizeof(uint32_t), dst.data() + offset + 4);

        // trim any excess space from the vector
        dst.resize(offset + 8 + dst_size);

        return 8 + dst_size;
    }

    Lz4Decompressor::Lz4Decompressor()
    {
        _lz4_stream = LZ4_createStreamDecode();
    }

    Lz4Decompressor::~Lz4Decompressor()
    {
        LZ4_freeStreamDecode(_lz4_stream);
    }


    uint32_t
    Lz4Decompressor::decompress_raw(const std::vector<char> &src, std::shared_ptr<std::vector<char>> dst, int offset)
    {
        // decompress the block
        int size = LZ4_decompress_safe_continue(_lz4_stream, src.data(),
                                                dst->data()+offset, src.size(),
                                                dst->size()-offset);
        if (size <= 0) {
            throw ValidationError("Error decompressing data");
        }
        if (size != dst->size()) {
            throw ValidationError("Unexpected decompression size while decompressing data");
        }

        // read the full compressed data and 2 4-byte sizes
        return size;
    }

    uint32_t
    Lz4Decompressor::decompress_block(const char *src, std::vector<char> &dst)
    {
        // get the compressed size of the block
        uint32_t src_size, dst_size;
        std::copy_n(src, sizeof(uint32_t), reinterpret_cast<char *>(&dst_size));

        // check if this block is empty
        if (dst_size == 0) {
            return 4; // nothing to copy, return the number of bytes read
        }

        // get the decompressed size of the block
        std::copy_n(src + 4, sizeof(uint32_t), reinterpret_cast<char *>(&src_size));

        // resize to fit the decompressed data
        dst.resize(dst_size);

        // decompress the block
        int size = LZ4_decompress_safe_continue(_lz4_stream, src, dst.data(), src_size, dst_size);
        if (size <= 0) {
            throw ValidationError("Error decompressing data");
        }
        if (size != dst_size) {
            throw ValidationError("Unexpected decompression size while decompressing data");
        }

        // read the full compressed data and 2 4-byte sizes
        return src_size + 8;
    }
}

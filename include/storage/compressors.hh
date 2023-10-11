#pragma once

#include <lz4.h>
#include <vector>

namespace springtail {
    /** Interface for compression objects. */
    class Compressor {
    public:
        virtual ~Compressor()
        { }

        /** Compresses the provided bytes.  Call multiple times for compressing multiple disjoint
            blocks into a single compressed block using a shared dictionary.  Returns the number of
            bytes appended to the dst vector. */
        virtual uint32_t compress_block(const std::vector<char> &src, std::vector<char> &dst) = 0;
    };

    /** Interface for decompression objects. */
    class Decompressor {
    public:
        virtual ~Decompressor()
        { }

        virtual uint32_t decompress_block(const char *src, std::vector<char> &dst) = 0;
    };

    /** Compressor implementation for Lz4. */
    class Lz4Compressor : public Compressor {
    private:
        LZ4_stream_t *_lz4_stream;

    public:
        Lz4Compressor();
        ~Lz4Compressor();

        uint32_t compress_block(const std::vector<char> &src, std::vector<char> &dst);
    };

    /** Decompressor implementation for Lz4. */
    class Lz4Decompressor : public Decompressor {
    private:
        LZ4_streamDecode_t *_lz4_stream;

    public:
        Lz4Decompressor();
        ~Lz4Decompressor();

        uint32_t decompress_block(const char *src, std::vector<char> &dst);
    };
}

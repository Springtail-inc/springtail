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

        /** Compresses bytes in src into destination, no lengths are encoded */
        virtual uint32_t compress_raw(std::shared_ptr<std::vector<char>> src, std::vector<char> &dst) = 0;

        /** Reset the stream; call prior to compressing a new stream using same compressor */
        virtual void reset_stream() = 0;
    };

    /** Interface for decompression objects. */
    class Decompressor {
    public:
        virtual ~Decompressor()
        { }

        virtual uint32_t decompress_block(const char *src, std::vector<char> &dst) = 0;
        virtual uint32_t decompress_raw(const std::vector<char> &src, std::shared_ptr<std::vector<char>> dst, int offset) = 0;
    };

    /** Compressor implementation for Lz4. */
    class Lz4Compressor : public Compressor {
    private:
        LZ4_stream_t *_lz4_stream;

    public:
        Lz4Compressor();
        ~Lz4Compressor();

        uint32_t compress_block(const std::vector<char> &src, std::vector<char> &dst);
        uint32_t compress_raw(std::shared_ptr<std::vector<char>> src, std::vector<char> &dst);
        void reset_stream();
    };

    /** Decompressor implementation for Lz4. */
    class Lz4Decompressor : public Decompressor {
    private:
        LZ4_streamDecode_t *_lz4_stream;

    public:
        Lz4Decompressor();
        ~Lz4Decompressor();

        uint32_t decompress_block(const char *src, std::vector<char> &dst);
        uint32_t decompress_raw(const std::vector<char> &src, std::shared_ptr<std::vector<char>> dst, int offset);
    };
}

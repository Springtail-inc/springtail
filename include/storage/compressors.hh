#pragma once

#include <lz4.h>
#include <vector>

namespace springtail {

    /**
     * @brief Interface for compression objects.
     */
    class Compressor {
    public:
        virtual ~Compressor()
        { }

        /**
         * @brief Deprecated do not use
         * @details Compresses the provided bytes.  Call multiple times for compressing multiple disjoint
         * blocks into a single compressed block using a shared dictionary.  Returns the number of
         * bytes appended to the dst vector.
         */
        virtual uint32_t compress_block(const std::vector<char> &src, std::vector<char> &dst) = 0;

        /**
         * @brief Compress a source data vector into a destination vector
         * @param src  Source data vector
         * @param dst  Destination data vector
         * @return uint32_t Compressed size in bytes
         */
         virtual uint32_t compress_raw(std::shared_ptr<std::vector<char>> src, std::vector<char> &dst) = 0;

        /**
         * @brief  Reset the stream; call prior to compressing a new stream using same compressor
         */
        virtual void reset_stream() = 0;    
    };

    /** 
     * @brief Interface for decompression objects.
     */
    class Decompressor {
    public:
        virtual ~Decompressor()
        { }

        /**
         * @brief Deprecated do not use
         */
        virtual uint32_t decompress_block(const char *src, std::vector<char> &dst) = 0;

        /**
         * @brief Decompress block, assume vectors are correct size
         * @param src    src buffer holding compressed data
         * @param dst    dst buffer empty, sized to full uncompressed size
         * @param offset offset within dst buffer for this data
         * @return size of uncompressed data
         */
        virtual uint32_t decompress_raw(const std::vector<char> &src, std::shared_ptr<std::vector<char>> dst, int offset) = 0;
    };

    /** 
     * @brief Compressor implementation for Lz4.
     */
    class Lz4Compressor : public Compressor {
    private:
        LZ4_stream_t *_lz4_stream; //!< underlying lz4 compression stream

    public:
        Lz4Compressor();
        ~Lz4Compressor();

        /**
         * @brief Deprecated do not use
         */
        uint32_t compress_block(const std::vector<char> &src, std::vector<char> &dst);

        /**
         * @brief Compress a source data vector into a destination vector
         * @param src  Source data vector
         * @param dst  Destination data vector
         * @return uint32_t Compressed size in bytes
         */
        uint32_t compress_raw(std::shared_ptr<std::vector<char>> src, std::vector<char> &dst);

        /**
         * @brief  Reset the stream; call prior to compressing a new stream using same compressor
         */
        void reset_stream();
    };

    /** 
     * @brief Decompressor implementation for Lz4.
     */
    class Lz4Decompressor : public Decompressor {
    private:
        LZ4_streamDecode_t *_lz4_stream;  //!< underlying compression stream

    public:
        Lz4Decompressor();
        ~Lz4Decompressor();

        /**
         * @brief Deprecated do not use
         */
        uint32_t decompress_block(const char *src, std::vector<char> &dst);

        /**
         * @brief Decompress block, assume vectors are correct size
         * @param src    src buffer holding compressed data
         * @param dst    dst buffer empty, sized to full uncompressed size
         * @param offset offset within dst buffer for this data
         * @return size of uncompressed data
         */
        uint32_t decompress_raw(const std::vector<char> &src, std::shared_ptr<std::vector<char>> dst, int offset);

    };
}

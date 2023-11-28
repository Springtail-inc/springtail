// #include <common/logging.hh>
#include <xxhash.h>

#include <storage/compressors.hh>
#include <storage/extent.hh>

namespace springtail {

    std::shared_ptr<CompressedExtent>
    Extent::compress(std::shared_ptr<Compressor> compressor)
    {
        std::vector<char> compressed_data;

        // serialize the header
        std::vector<char> &&header_data = _header.pack();

        // compress each vector
        compressor->compress_block(header_data, compressed_data);
        compressor->compress_block(_fixed_data, compressed_data);
        compressor->compress_block(_variable_data, compressed_data);

        // make space for the checksum
        uint32_t offset = compressed_data.size();
        compressed_data.resize(offset + 4);

        // generate a checksum of the data and store it at the end
        uint32_t checksum = XXH32(reinterpret_cast<char *>(compressed_data.data()), compressed_data.size() - 4, 0);
        std::copy_n(reinterpret_cast<char *>(&checksum), sizeof(uint32_t), compressed_data.data() + offset);

        std::cerr << "compress()" << std::endl
                  << "\t" << _fixed_data.size() << std::endl
                  << "\t" << _variable_data.size() << std::endl
                  << "\t" << checksum << std::endl
                  << "\tcompressed size: " << compressed_data.size() << std::endl;

        return std::make_shared<CompressedExtent>(compressed_data);
    }

    std::shared_ptr<Extent>
    CompressedExtent::decompress(std::shared_ptr<Decompressor> decompressor,
                                 bool verify_checksum)
    {
        // optionally verify the checksum here
        uint32_t checksum;
        std::copy_n(_data.data() + _data.size() - 4, sizeof(uint32_t),
                    reinterpret_cast<char *>(&checksum));
        if (verify_checksum) {
            uint32_t c = XXH32(reinterpret_cast<char *>(_data.data()), _data.size() - 4, 0);
            if (c != checksum) {
                std::cerr << "Checksum mis-match" << std::endl;
                throw ValidationError("Checksum mismatch");
            }
        }

        // decompress the data
        uint32_t offset = 0;
        std::vector<char> header_data, fixed_data, variable_data, untyped_data, untyped_keys;
        offset += decompressor->decompress_block(_data.data() + offset, header_data);
        offset += decompressor->decompress_block(_data.data() + offset, fixed_data);
        offset += decompressor->decompress_block(_data.data() + offset, variable_data);

        // create the extent and return it
        return std::make_shared<Extent>(header_data, fixed_data, variable_data);
    }

}

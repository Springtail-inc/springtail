#pragma once

#include <memory>
#include <vector>

namespace springtail {

/**
 * Helper class to handle the variable size data in an Extent without performing re-allocs caused by
 * resizing std::vector.  This ensures that the std::string_view references in the variable_hash of
 * the Extent are never accidentally invalidated.
 */
class VariableData {
private:
    constexpr static uint32_t ChunkSize = 4096;

    struct ChunkInfo {
        std::vector<char> data;
        uint32_t cumulative_end;  // end offset of this chunk in the global data
    };

public:
    struct Position {
        const char *data;
        uint32_t offset;
    };

public:
    VariableData() = default;

    VariableData(const VariableData &data)
    {
        _chunks.reserve(data._chunks.size());
        for (auto &chunk_info : data._chunks) {
            std::vector<char> new_chunk;
            new_chunk.reserve(chunk_info.data.capacity());
            new_chunk = chunk_info.data;  // Copy size and contents
            _chunks.push_back({std::move(new_chunk), chunk_info.cumulative_end});
        }
    }

    VariableData(VariableData &&data) = default;

    VariableData(std::vector<char> &&data)
    {
        uint32_t size = data.size();
        _chunks.push_back({std::move(data), size});
    }

    Position push_back(const char *buffer, uint32_t size) {
        uint32_t total_size = 4 + size;
        char *dest = nullptr;
        uint32_t offset_in_chunk = 0;
        uint32_t global_offset;

        // Check if we can fit in the last chunk
        if (!_chunks.empty()) {
            auto &last_chunk = _chunks.back();
            uint32_t available = last_chunk.data.capacity() - last_chunk.data.size();

            if (available >= total_size) {
                // Reuse existing chunk
                offset_in_chunk = last_chunk.data.size();
                global_offset = last_chunk.cumulative_end;
                last_chunk.data.resize(last_chunk.data.size() + total_size);
                dest = last_chunk.data.data() + offset_in_chunk;
                last_chunk.cumulative_end += total_size;
            } else {
                // Need new chunk
                uint32_t chunk_capacity = std::max(ChunkSize, total_size);
                std::vector<char> new_chunk;
                new_chunk.reserve(chunk_capacity);
                new_chunk.resize(total_size);
                offset_in_chunk = 0;
                global_offset = last_chunk.cumulative_end;
                dest = new_chunk.data();
                _chunks.push_back({std::move(new_chunk), global_offset + total_size});
            }
        } else {
            // First chunk
            uint32_t chunk_capacity = std::max(ChunkSize, total_size);
            std::vector<char> new_chunk;
            new_chunk.reserve(chunk_capacity);
            new_chunk.resize(total_size);
            offset_in_chunk = 0;
            global_offset = 0;
            dest = new_chunk.data();
            _chunks.push_back({std::move(new_chunk), total_size});
        }

        // Write data
        std::memcpy(dest, &size, 4);
        std::memcpy(dest + 4, buffer, size);

        return Position{ dest, global_offset };
    }

    const char *data(uint32_t offset) {
        // Binary search for the chunk containing this offset
        auto it = std::lower_bound(_chunks.begin(), _chunks.end(), offset,
                                   [](const ChunkInfo &chunk, uint32_t value) {
                                       return chunk.cumulative_end <= value;
                                   });

        if (it == _chunks.end()) {
            return nullptr;  // Invalid offset
        }

        // Calculate start offset of this chunk by subtracting its used size
        uint32_t chunk_end = it->cumulative_end;
        uint32_t chunk_start = chunk_end - it->data.size();

        // Offset within chunk
        uint32_t offset_in_chunk = offset - chunk_start;

        return it->data.data() + offset_in_chunk;
    }

    void copy_into(char *buffer) {
        auto pos = 0;
        for (auto &chunk : _chunks) {
            std::memcpy(buffer + pos, chunk.data.data(), chunk.data.size());
            pos += chunk.data.size();
        }
    }

    void populate_hash(std::unordered_map<std::string_view, uint32_t> &hash) {
        uint32_t global_offset = 0;

        // go through each chunk
        for (auto &chunk_info : _chunks) {
            uint32_t pos = 0;
            const char *chunk = chunk_info.data.data();
            uint32_t chunk_used = chunk_info.data.size();

            while (pos + 4 <= chunk_used) {
                uint32_t size;
                std::memcpy(&size, chunk + pos, 4);

                // Safety check to prevent reading past end of chunk
                if (pos + 4 + size > chunk_used) {
                    break;  // Corrupted data or incomplete entry
                }

                std::string_view str(chunk + pos + 4, size);

                // populate the hash entry with global offset
                hash[str] = global_offset + pos;

                pos += 4 + size;
            }

            // update global offset to the end of this chunk
            global_offset += chunk_used;
        }
    }

    uint32_t size() const {
        return _chunks.empty() ? 0 : _chunks.back().cumulative_end;
    }

private:
    std::vector<ChunkInfo> _chunks;
};

}

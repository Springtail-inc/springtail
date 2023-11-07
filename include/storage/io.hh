#pragma once

#include <string>
#include <vector>
#include <functional>
#include <filesystem>

#include <storage/io_request.hh>

namespace springtail {

    class IOHandle {
    private:
        std::filesystem::path _path;
        int _mode;
        bool _is_compressed;

    public:
        IOHandle(std::filesystem::path path, int mode, bool is_compressed) :
            _path(path), _mode(mode), _is_compressed(is_compressed) {};

        ~IOHandle() {};

        int get_mode() { return _mode; }

        std::string get_path() { return _path.string(); }

        // async operations; calls callback
        void read(uint64_t pos, io_read_callback_fn callback);

        void append(const char *buffer, int length, io_write_callback_fn callback);

        void append(const std::vector<char> &data, io_write_callback_fn callback);

        void append(const std::vector<char> data[], uint8_t count, io_write_callback_fn callback);

        void append(const std::vector<std::vector<char>> data, io_write_callback_fn callback);

        void write(uint64_t offset, const std::vector<char> &data, io_write_callback_fn callback);

        void sync(io_status_callback_fn callback);

        // no close, delete FH
    };
}
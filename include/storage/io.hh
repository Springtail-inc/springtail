#pragma once

#include <string>
#include <vector>
#include <functional>
#include <filesystem>
#include <memory>
#include <future>

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
        std::future<std::shared_ptr<IOResponseRead>>
        read(uint64_t pos, io_read_callback_fn callback);

        std::future<std::shared_ptr<IOResponseAppend>>
        append(const char *buffer, int length, io_append_callback_fn callback);
        
        std::future<std::shared_ptr<IOResponseAppend>>
        append(std::shared_ptr<std::vector<char>> data, io_append_callback_fn callback);

        std::future<std::shared_ptr<IOResponseAppend>>
        append(std::shared_ptr<std::vector<char>> data[], uint8_t count, io_append_callback_fn callback);

        std::future<std::shared_ptr<IOResponseAppend>>
        append(const std::vector<std::shared_ptr<std::vector<char>>> &data, io_append_callback_fn callback);

        std::future<std::shared_ptr<IOResponseWrite>>
        write(uint64_t offset, std::shared_ptr<std::vector<char>> data, io_write_callback_fn callback);

        std::future<std::shared_ptr<IOResponseWrite>>
        write(uint64_t offset, std::vector<std::shared_ptr<std::vector<char>>> data, io_write_callback_fn callback);

        std::future<std::shared_ptr<IOResponse>>
        sync(io_status_callback_fn callback);

        // sync operations, no callback blocks until completion
        std::shared_ptr<IOResponseRead>
        read(uint64_t pos);

        std::shared_ptr<IOResponseAppend>
        append(const char *buffer, int length);
        
        std::shared_ptr<IOResponseAppend>
        append(std::shared_ptr<std::vector<char>> data);

        std::shared_ptr<IOResponseAppend>
        append(std::shared_ptr<std::vector<char>> data[], uint8_t count);

        std::shared_ptr<IOResponseAppend>
        append(const std::vector<std::shared_ptr<std::vector<char>>> &data);

        std::shared_ptr<IOResponseWrite>
        write(uint64_t offset, std::shared_ptr<std::vector<char>> data);

        std::shared_ptr<IOResponseWrite>
        write(uint64_t offset, std::vector<std::shared_ptr<std::vector<char>>> data);

        std::shared_ptr<IOResponse>
        sync();

        // no close, FH cache closes handles as necessary
    };
}
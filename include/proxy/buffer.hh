#pragma once

#include <vector>
#include <cassert>
#include <string>

#include <pg_repl/pg_types.hh>

namespace springtail {
    class ProxyBuffer {
    public:
        ProxyBuffer(int initial_size) : _capacity(initial_size) {
            _buffer.resize(initial_size);
        }

        void reset() { _offset = 0; _data_size = 0; }

        void resize(int size) {
            _buffer.resize(size);
            _capacity = size;
        }

        int offset() const {
            return _offset;
        }

        int size() const {
            return _offset;
        }

        int capacity() const {
            return _capacity;
        }

        void set_read(int n) {
            _data_size += n;
        }

        int remaining() const {
            return _data_size - _offset;
        }

        char *data() {
            return _buffer.data();
        }

        char *next_data() {
            return _buffer.data() + _data_size;
        }

        const char *current_data() {
            return _buffer.data() + _offset;
        }

        void put(char byte) {
            assert(_offset < _capacity);
            sendint8(byte, _buffer.data() + _offset);
            _offset++;
        }

        void put16(uint16_t i) {
            assert(_offset + 2 <= _capacity);
            sendint16(i, _buffer.data() + _offset);
            _offset += 2;
        }

        void put32(uint32_t i) {
            assert(_offset + 4 <= _capacity);
            sendint32(i, _buffer.data() + _offset);
            _offset += 4;
        }

        void putString(const std::string_view s) {
            assert(_offset + s.size() + 1 <= _capacity);
            memcpy(_buffer.data() + _offset, s.data(), s.size() + 1); // copy null byte
            _offset += s.size() + 1;
        }

        void putBytes(const char *bytes, int size) {
            assert(_offset + size <= _capacity);
            memcpy(_buffer.data() + _offset, bytes, size);
            _offset += size;
        }

        int8_t get() {
            assert(_offset + 1  <= _data_size);
            int8_t i = recvint8(_buffer.data() + _offset);
            _offset++;
            return i;
        }

        int16_t get16() {
            assert(_offset + 2  <= _data_size);
            int16_t i = recvint16(_buffer.data() + _offset);
            _offset += 2;
            return i;
        }

        int32_t get32() {
            assert(_offset + 4  <= _data_size);
            int32_t i = recvint32(_buffer.data() + _offset);
            _offset += 4;
            return i;
        }

        std::string getString() {
            int str_len = ::strnlen(_buffer.data() + _offset, _data_size - _offset);
            assert(str_len < _data_size - _offset);
            if (str_len < _data_size - _offset) {
                std::string s(_buffer.data() + _offset);
                _offset += str_len + 1; // skip null byte
                return s;
            }
            return {};
        }

        void getBytes(char *bytes, int32_t len) {
            assert(_offset + len <= _data_size);
            memcpy(bytes, _buffer.data() + _offset, len);
            _offset += len;
        }

        std::string getBytes(int32_t len) {
            assert(_offset + len <= _data_size);
            std::string s(_buffer.data() + _offset, len);
            _offset += len;
            return s;
        }

    private:
        std::vector<char> _buffer;
        int _offset = 0;
        int _capacity = 0;
        int _data_size = 0;
    };
}
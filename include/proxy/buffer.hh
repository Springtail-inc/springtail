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

        int size() {
            return _offset;
        }

        int capacity() {
            return _capacity;
        }

        void set_read(int n) {
            _data_size += n;
        }

        int remaining() {
            return _data_size - _offset;
        }

        char *data() {
            return _buffer.data();
        }

        char *next_data() {
            return _buffer.data() + _data_size;
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
/*
        void putString(const std::string &s) {
            assert(_offset + s.size() + 1 <= _capacity);
            memcpy(_buffer.data() + _offset, s.c_str(), s.size() + 1); // copy null byte
            _offset += s.size() + 1;
        } */

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
            int8_t i = recvint8(_buffer.data() + _offset);
            _offset++;
            return i;
        }

        int16_t get16() {
            int16_t i = recvint16(_buffer.data() + _offset);
            _offset += 2;
            return i;
        }

        int32_t get32() {
            int32_t i = recvint32(_buffer.data() + _offset);
            _offset += 4;
            return i;
        }

        std::string getString() {
            // read until we get a null byte
            std::string s;
            while (_buffer[_offset] != 0) {
                s.push_back(_buffer[_offset]);
                _offset++;
            }
            _offset++; // skip null byte
            return s;
        }

        std::string getBytes(int32_t len) {
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
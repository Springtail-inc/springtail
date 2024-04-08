#pragma once

#include <vector>

#include <pg_repl/pg_types.hh>

namespace springtail {
    class ProxyBuffer {
    public:
        ProxyBuffer(int initial_size) {
            _buffer.resize(initial_size);
        }

        void reset() { _offset = 0; }

        void resize(int size) {
            _buffer.resize(size);
        }

        int size() {
            return _offset;
        }

        int capacity() {
            return _buffer.size();
        }

        char *data() {
            return _buffer.data();
        }

        void put(char byte) {
            sendint8(byte, _buffer.data() + _offset);
            _offset++;
        }

        void put16(uint16_t i) {
            sendint16(i, _buffer.data() + _offset);
            _offset += 2;
        }

        void put32(uint32_t i) {
            sendint32(i, _buffer.data() + _offset);
            _offset += 4;
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

    private:
        std::vector<char> _buffer;
        int _offset = 0;
    };  
}
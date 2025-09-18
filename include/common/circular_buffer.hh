#pragma once

#include <vector>

namespace springtail {

    /**
     * @brief Circular buffer of elements of type T. 
     * It has a FIFO queue semantic.
     */
    template< typename T >
    struct CircularBuffer {
        typedef T value_type;

        explicit CircularBuffer(size_t max_size) {
            _b.resize(max_size);
        }

        /** Returns true if the buffer is empty.  */
        bool empty() const {
            return _cnt == 0;
        }

        /** Returns the number of elements in the buffer.  */
        size_t size() const {
            return _cnt;
        }

        /** Add an element into the buffer. The buffer must have
         *  space available or the behavior is undefined. 
         */
        void put( T v ) {
            DCHECK(_cnt < _b.size());
            DCHECK(_read_pnt < _b.size());
            auto i = ( _read_pnt + _cnt ) % _b.size();
            _b[i] = v;
            ++_cnt;
        }

        /** Get the next element. The buffer must not be empty
         *  or the behavior is undefined. 
         */
        T next() {
            DCHECK(!empty());
            --_cnt;
            auto i = _read_pnt++;
            if (_read_pnt == _b.size())
                _read_pnt = 0;
            return _b[i];
        }

        /** Get elements by index */
        const T& operator[](size_t i) const {
            DCHECK(i < size());
            return _b[( _read_pnt + i ) % _b.size()];
        }

    private:
        std::vector<value_type> _b;
        size_t _cnt = 0; // number of elements in the buffer
        size_t _read_pnt = 0;
    };

}

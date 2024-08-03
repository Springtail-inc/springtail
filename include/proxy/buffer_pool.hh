#pragma once

#include <memory>
#include <cassert>
#include <mutex>
#include <atomic>
#include <vector>
#include <thread>
#include <condition_variable>
#include <queue>

#include <pg_repl/pg_types.hh>

namespace springtail {
namespace pg_proxy {

    class Buffer;
    using BufferPtr = std::shared_ptr<Buffer>;

    /**
     * @brief Class representing a pool of buffers, currently fairly simple.
     */
    class BufferPool {
    public:
        constexpr static int BIG_ALLOC_QUEUE_LIMIT = 32 * 1024 * 1024; ///< 32MB limit on allocations before queueing
        constexpr static int BIG_ALLOCS_ALLOWED = 1; ///< 1 thread can have a large buffer allocated at a time

        BufferPool() {}

        BufferPool(const BufferPool&) = delete;
        BufferPool(const BufferPool&&) = delete;
        BufferPool& operator=(const BufferPool&) = delete;

        static BufferPool *get_instance();

        /**
         * @brief Get a buffer from the pool, if available, otherwise allocate a new one.
         * If the size is greater than BIG_ALLOC_QUEUE_LIMIT and
         * @param size size of buffer to get
         * @return BufferPtr buffer
         */
        BufferPtr get(int size);

        /**
         * @brief Indicates the release of a buffer.
         * If the size is greater than BIG_ALLOC_QUEUE_LIMIT, notify a waiting thread.
         * @param size size of buffer to release
         */
        void release(int size);


    private:
        struct Waiter {
            std::condition_variable cv;
            bool done = false;
        };
        using WaiterPtr = std::shared_ptr<Waiter>;

        std::mutex _alloc_mutex;
        std::queue<WaiterPtr> _big_alloc_queue;
        int _big_alloc_count = 0;

        static std::once_flag _init_flag;
        static BufferPool *_instance;
        static BufferPool *_init();
    };
    using BufferPoolPtr = std::shared_ptr<BufferPool>;


    /** Class representing a single buffer, holding a single packet/message worth of data */
    class Buffer : public std::enable_shared_from_this<Buffer> {
    public:
        /** Construct a buffer from a pre-allocated buffer, optionally specifying data size */
        Buffer(char *buffer, int capacity, int data_size=0)
            : _capacity(capacity), _data_size(data_size),
              _buffer(buffer), _dynamic(false)
        {}

        Buffer(const Buffer&) = delete;
        Buffer& operator=(const Buffer&) = delete;
        Buffer(Buffer&&) = delete;
        Buffer& operator=(Buffer&&) = delete;

        ~Buffer() {
            if (_buffer && _dynamic) {
                delete[] _buffer;
            }
        }

        /** Reset internals of buffer, but not capacity */
        void reset() {
            _offset = 0;
            _data_size = 0;
        }

        /** Capacity of buffer, not size of contents */
        int capacity() const {
            return _capacity;
        }

        /** Size of data contents -- not capacity */
        int size() const {
            return _data_size;
        }

        /** Remaining bytes in buffer (of real data) */
        int remaining() const {
            return _data_size - _offset;
        }

        /** Pointer to current data based on current offset */
        char *current_data() const {
            return _buffer + _offset;
        }

        char *data() {
            return _buffer;
        }

        void copy_into(char *src, int size) {
            assert(size <= _capacity);
            memcpy(_buffer + _data_size, src, size);
            _data_size += size;
        }

        /** Set the size of data contents, e.g., if data is copied in */
        void set_size(int size) {
            assert (size <= _capacity);
            _data_size = size;
        }

        void incr_size(int size) {
            assert(_data_size + size <= _capacity);
            _data_size += size;
        }

        void put(char byte) {
            assert(_offset < _capacity);
            sendint8(byte, _buffer + _offset);
            _offset++;
            _data_size++;
        }

        void put16(uint16_t i) {
            assert(_offset + 2 <= _capacity);
            sendint16(i, _buffer + _offset);
            _offset += 2;
            _data_size += 2;
        }

        void put32(uint32_t i) {
            assert(_offset + 4 <= _capacity);
            sendint32(i, _buffer + _offset);
            _offset += 4;
            _data_size += 4;
        }

        void put_string(const std::string_view s) {
            int ssize = s.size();
            assert(_offset + ssize + 1 <= _capacity);
            memcpy(_buffer + _offset, s.data(), ssize + 1); // copy null byte
            _offset += ssize + 1;
            _data_size += ssize + 1;
        }

        void put_bytes(const char *bytes, int size) {
            assert(_offset + size <= _capacity);
            memcpy(_buffer + _offset, bytes, size);
            _offset += size;
            _data_size += size;
        }

        int8_t get() {
            assert(_offset + 1  <= _data_size);
            int8_t i = recvint8(_buffer + _offset);
            _offset++;
            return i;
        }

        int16_t get16() {
            assert(_offset + 2  <= _data_size);
            int16_t i = recvint16(_buffer + _offset);
            _offset += 2;
            return i;
        }

        int32_t get32() {
            assert(_offset + 4  <= _data_size);
            int32_t i = recvint32(_buffer + _offset);
            _offset += 4;
            return i;
        }

        std::string_view get_string() {
            int str_len = ::strnlen(_buffer + _offset, _data_size - _offset);
            assert(str_len < _data_size - _offset);
            std::string_view s(_buffer + _offset, str_len);
            _offset += str_len + 1; // skip null byte
            return s;
        }

        void get_bytes(char *bytes, int32_t len) {
            assert(_offset + len <= _data_size);
            memcpy(bytes, _buffer + _offset, len);
            _offset += len;
        }

        std::string_view get_bytes(int32_t len) {
            assert(_offset + len <= _data_size);
            std::string_view s(_buffer + _offset, len);
            _offset += len;
            return s;
        }

        friend BufferPtr BufferPool::get(int size);

    private:
        /** Dynamically allocate a buffer of a certain size */
        Buffer(int size) : _capacity(size), _dynamic(true) { _buffer = new char[size]; }

        /** Factory method to create a buffer */
        static BufferPtr create(int size) {
            struct make_shared_enabler : public Buffer {
                make_shared_enabler(int size) : Buffer(size) {}
            };
            return std::make_shared<make_shared_enabler>(size);
        }

        /** capacity in bytes of buffer, max it can hold */
        int _capacity=0;
        /** current read/write offset into the buffer, for next put/get */
        int _offset=0;
        /** actual amount of valid data in the buffer */
        int _data_size=0;
        /** buffer data */
        char *_buffer = nullptr;
        /** is this buffer dynamically allocated */
        bool _dynamic = false;
    };

    class BufferList {
    public:
        BufferList() = default;
        BufferList(const BufferList&) = delete;
        BufferList(const BufferList&&) = delete;
        BufferList& operator=(const BufferList&) = delete;

        BufferPtr get(int size) {
            BufferPtr buffer = BufferPool::get_instance()->get(size);
            buffers.push_back(buffer);
            return buffer;
        }

        std::vector<BufferPtr> buffers;
    };
} // namespace pg_proxy
} // namespace springtail
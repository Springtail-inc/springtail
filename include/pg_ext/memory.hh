#pragma once

#include <pg_ext/export.hh>

#include <list>
#include <string_view>
#include <memory>
#include <map>
#include <absl/container/flat_hash_map.h>

//// EXPORTED INTERFACES

// Context management
extern "C" PGEXT_API void *CurrentMemoryContext; // NOSONAR - Memory context should be global

extern "C" PGEXT_API void *AllocSetContextCreateInternal(void *parent,
                                                         const char *name,
                                                         size_t minContextSize,
                                                         size_t initBlockSize,
                                                         size_t maxBlockSize);
extern "C" PGEXT_API void *MemoryContextAlloc(void *context, size_t size);
extern "C" PGEXT_API void *MemoryContextAllocZero(void *context, size_t size);
extern "C" PGEXT_API void MemoryContextDelete(void *context);

// Memory management
extern "C" PGEXT_API void *palloc(size_t size);
extern "C" PGEXT_API void *palloc0(size_t size);
extern "C" PGEXT_API void pfree(void *ptr);
extern "C" PGEXT_API void *repalloc(void *ptr, size_t size);

//// INTERNAL INTERFACES

/**
 * Object to manage memory in a way similar to postgres.  Used to implement the PG emulation
 * functions to support extensions.
 */
class MemoryContext {
public:
    MemoryContext(MemoryContext* parent,
                  std::string_view name,
                  size_t init_size,
                  size_t max_size);

    void *alloc(size_t size);
    void *alloc0(size_t size);

    void clear();

    // Helper methods for context management
    MemoryContext* create_child(std::string_view name, size_t init_size, size_t max_size);
    void remove_child(MemoryContext* child);

    MemoryContext* parent() const { return _parent; }

    // Memory management
    bool free(void* ptr);

private:
    /** Definition of a memory block. */
    struct MemoryBlock {
        explicit MemoryBlock(size_t block_size) {
            memory = static_cast<char*>(std::malloc(block_size)); // NOSONAR - malloc is used for memory management for pg
            if (!memory) {
                throw std::bad_alloc();
            }
            size = block_size;
            pos = 0;
        }

        ~MemoryBlock() {
            if (memory) {
                std::free(memory); // NOSONAR - free is used for memory management for pg
                memory = nullptr;
            }
        }

        // Prevent copying
        MemoryBlock(const MemoryBlock&) = delete;
        MemoryBlock& operator=(const MemoryBlock&) = delete;

        // Allow moving
        MemoryBlock(MemoryBlock&& other) noexcept
            : memory(other.memory), size(other.size), pos(other.pos) {
            other.memory = nullptr;
            other.size = 0;
            other.pos = 0;
        }

        MemoryBlock& operator=(MemoryBlock&& other) noexcept {
            if (this != &other) {
                if (memory) {
                    std::free(memory); // NOSONAR - free is used for memory management for pg
                }
                memory = other.memory;
                size = other.size;
                pos = other.pos;
                other.memory = nullptr;
                other.size = 0;
                other.pos = 0;
            }
            return *this;
        }

        char *memory; ///< The actual memory of this block
        size_t size; ///< The size of this block
        size_t pos; ///< The position up to which we have allocated memory from this block

        size_t remaining() const { return size - pos; }
    };

    using BlockList = std::list<std::unique_ptr<MemoryBlock>>;
    using BlockMap = std::map<size_t, std::unique_ptr<MemoryBlock>>;
    using LargeAllocMap = absl::flat_hash_map<char*, std::unique_ptr<MemoryBlock>>;

    BlockMap _blocks; ///< A map of remaining space to blocks that still have free space
    BlockList _full_blocks; ///< A list of blocks that are completely allocated
    LargeAllocMap _large_allocs; ///< Holds large memory allocations (larger than _max_size)

    std::string _name; ///< The name of the block; used for debugging
    size_t _init_size; ///< The default size of a block and the initial size of the first block
    size_t _max_size; ///< The max size of a block

    /** The child contexts of this context.  Automatically freed if this context is freed. */
    std::list<std::unique_ptr<MemoryContext>> _children;
    MemoryContext* _parent; ///< The parent context of this context.  If nullptr, this is the top context.

    void* _alloc_large(size_t size);
};

/**
 * C++ allocator that uses palloc() and pfree().
 */
template <typename T>
struct PGAllocator {
    using value_type = T;

    PGAllocator() noexcept = default;
    template <class U> explicit PGAllocator(const PGAllocator<U>&) noexcept {}

    T* allocate(std::size_t n) {
        if (n > std::size_t(-1) / sizeof(T))
            throw std::bad_alloc();
        void* ptr = palloc(n * sizeof(T));
        if (!ptr)
            throw std::bad_alloc();
        return static_cast<T*>(ptr);
    }

    void deallocate(T* p, std::size_t) noexcept {
        pfree(p);
    }

    // Optional: allocator traits
    template <typename U>
    struct rebind {
        using other = PGAllocator<U>;
    };

    // Required for some STL implementations (e.g. libstdc++)
    using pointer = T*;
    using const_pointer = const T*;
    using reference = T&;
    using const_reference = const T&;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;
};

// Equality (required by standard)
template <typename T, typename U>
bool operator==(const PGAllocator<T>&, const PGAllocator<U>&) noexcept {
    return true;
}
template <typename T, typename U>
bool operator!=(const PGAllocator<T>&, const PGAllocator<U>&) noexcept {
    return false;
}

#pragma once

#include <list>
#include <string_view>
#include <memory>

#include <pg_ext/export.hh>

namespace pgext {

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

private:
    /** Definition of a memory block. */
    struct MemoryBlock {
        MemoryBlock(size_t block_size) {
            memory = static_cast<char*>(std::malloc(block_size));
            if (!memory) {
                throw std::bad_alloc();
            }
            size = block_size;
            pos = 0;
        }

        ~MemoryBlock() {
            if (memory) {
                std::free(memory);
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
                    std::free(memory);
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
    };

    using BlockList = std::list<std::unique_ptr<MemoryBlock>>;
    BlockList _blocks; ///< A list of blocks that still have free space
    BlockList _full_blocks; ///< A list of blocks that are completely allocated
    BlockList _large_allocs; ///< Holds large memory allocations (larger than _max_size)

    std::string _name; ///< The name of the block; used for debugging
    size_t _init_size; ///< The default size of a block and the initial size of the first block
    size_t _max_size; ///< The max size of a block

    /** The child contexts of this context.  Automatically freed if this context is freed. */
    std::list<std::unique_ptr<MemoryContext>> _children;
    MemoryContext* _parent; ///< The parent context of this context.  If nullptr, this is the top context.

    void* _alloc_large(size_t size);
};

} // namespace pgext 

//// EXPORTED INTERFACES

// Context management
extern "C" PGEXT_API void *CurrentMemoryContext;

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

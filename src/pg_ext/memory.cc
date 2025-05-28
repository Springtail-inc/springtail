#include <memory>
#include <absl/log/check.h>

#include <pg_ext/memory.hh>

namespace pgext {

// Define the global memory context
MemoryContext TopMemoryContext(nullptr, "TopMemoryContext", 8192, 1048576);

MemoryContext::MemoryContext(MemoryContext* parent,
                             std::string_view name,
                             size_t init_size,
                             size_t max_size)
    : _name(name),
      _init_size(init_size),
      _max_size(max_size),
      _parent(parent)
{
    // Create the initial block
    _blocks.push_back(std::make_unique<MemoryBlock>(init_size));
}

void*
MemoryContext::_alloc_large(size_t size)
{
    auto block = std::make_unique<MemoryBlock>(size);
    block->pos = size; // Mark as fully used
    
    _large_allocs.push_back(std::move(block));
    return _large_allocs.back()->memory;
}

void*
MemoryContext::alloc(size_t size)
{
    void *ptr;

    // check if it's a large block
    if (size > _max_size) {
        return _alloc_large(size);
    }

    // Pad size to be a multiple of sizeof(size_t) for proper alignment
    size_t aligned_size = (size + sizeof(size_t) - 1) & ~(sizeof(size_t) - 1);

    // Try to find space in existing blocks
    for (auto it = _blocks.begin(); it != _blocks.end();) {
        auto& block = *it;
        if (block->size - block->pos >= aligned_size) {
            ptr = block->memory + block->pos;
            block->pos += aligned_size;
            
            // If block is now full, move it to _full_blocks
            if (block->pos >= block->size) {
                _full_blocks.splice(_full_blocks.end(), _blocks, it++);
            }
            return ptr;
        }
        ++it;
    }

    // Allocate a new block
    size_t new_size = std::max(_init_size, aligned_size);
    _blocks.push_back(std::make_unique<MemoryBlock>(new_size));
    
    auto& block = _blocks.back();
    block->pos = aligned_size;
    ptr = block->memory;

    // If the new block is immediately full, move it to _full_blocks
    if (block->pos >= block->size) {
        _full_blocks.splice(_full_blocks.end(), _blocks, --_blocks.end());
    }

    return ptr;
}

void*
MemoryContext::alloc0(size_t size)
{
    auto ptr = this->alloc(size);
    if (ptr != nullptr) {
        std::memset(ptr, 0, size);
    }
    return ptr;
}

MemoryContext*
MemoryContext::create_child(std::string_view name, size_t init_size, size_t max_size)
{
    auto new_ctx = std::make_unique<MemoryContext>(this, name, init_size, max_size);
    auto raw_ptr = new_ctx.get();
    _children.push_back(std::move(new_ctx));
    return raw_ptr;
}

void
MemoryContext::clear()
{
    _blocks.clear();
    _full_blocks.clear();
    _large_allocs.clear();
    _children.clear();
}

void
MemoryContext::remove_child(MemoryContext* child)
{
    for (auto it = _children.begin(); it != _children.end(); ++it) {
        if (it->get() == child) {
            _children.erase(it);
            return;
        }
    }
}

} // namespace pgext

// Global memory context pointer
void* CurrentMemoryContext = &pgext::TopMemoryContext;

// Implementation of exported functions
void*
AllocSetContextCreateInternal(void *parent,
                              const char *name,
                              size_t minContextSize,
                              size_t initBlockSize,
                              size_t maxBlockSize)
{
    auto parent_ctx = static_cast<pgext::MemoryContext*>(parent);
    CHECK(parent_ctx != nullptr);

    return parent_ctx->create_child(
        name ? name : "UnnamedContext",
        initBlockSize,
        maxBlockSize
    );
}

void*
MemoryContextAlloc(void *context, size_t size)
{
    auto ctx = static_cast<pgext::MemoryContext*>(context);
    CHECK(ctx != nullptr);

    return ctx->alloc(size);
}

void*
MemoryContextAllocZero(void *context, size_t size)
{
    auto ctx = static_cast<pgext::MemoryContext*>(context);
    CHECK(ctx != nullptr);

    return ctx->alloc0(size);
}

void
MemoryContextDelete(void *context)
{
    auto ctx = static_cast<pgext::MemoryContext*>(context);
    CHECK(ctx != nullptr);

    // Clear all memory in this context
    ctx->clear();

    auto parent = ctx->parent();
    if (parent == nullptr) {
        return; // Don't delete top context
    }

    // Remove from parent
    parent->remove_child(ctx);
}

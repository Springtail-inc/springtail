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
    auto block = std::make_unique<MemoryBlock>(init_size);
    _blocks[block->remaining()] = std::move(block);
}

void*
MemoryContext::_alloc_large(size_t size)
{
    auto block = std::make_unique<MemoryBlock>(size);
    block->pos = size; // Mark as fully used

    char* memory = block->memory;
    _large_allocs[memory] = std::move(block);
    return memory;
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

    // Try to find a block with enough space
    auto it = _blocks.lower_bound(aligned_size);
    if (it != _blocks.end()) {
        auto& block = it->second;
        ptr = block->memory + block->pos;
        block->pos += aligned_size;
        CHECK(block->pos <= block->size);

        // If block is now full, move it to _full_blocks
        if (block->pos == block->size) {
            _full_blocks.push_back(std::move(block));
            _blocks.erase(it);
        } else {
            // Update the block's position in the map using node handle
            auto node = _blocks.extract(it);
            node.key() = block->remaining();
            _blocks.insert(std::move(node));
        }
        return ptr;
    }

    // Allocate a new block
    size_t new_size = std::max(_init_size, aligned_size);
    auto block = std::make_unique<MemoryBlock>(new_size);
    block->pos = aligned_size;
    ptr = block->memory;
    CHECK(block->pos <= block->size);

    // If the new block is immediately full, move it to _full_blocks
    if (block->pos == block->size) {
        _full_blocks.push_back(std::move(block));
    } else {
        _blocks[block->remaining()] = std::move(block);
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

bool
MemoryContext::free(void* ptr)
{
    if (ptr == nullptr) {
        return false;
    }

    auto it = _large_allocs.find(static_cast<char*>(ptr));
    if (it != _large_allocs.end()) {
        _large_allocs.erase(it);
        return true;
    }
    return false;
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

void*
palloc(size_t size)
{
    auto ctx = static_cast<pgext::MemoryContext*>(CurrentMemoryContext);
    CHECK(ctx != nullptr);
    return ctx->alloc(size);
}

void*
palloc0(size_t size)
{
    auto ptr = palloc(size);
    if (ptr != nullptr) {
        std::memset(ptr, 0, size);
    }
    return ptr;
}

void
pfree(void *ptr)
{
    if (ptr == nullptr) {
        return;
    }

    auto ctx = static_cast<pgext::MemoryContext*>(CurrentMemoryContext);
    CHECK(ctx != nullptr);

    // Try to free the pointer, if not found do nothing
    ctx->free(ptr);
}

void* repalloc(void* ptr, size_t size)
{
    if (ptr == nullptr) {
        return palloc(size);
    }

    auto ctx = static_cast<pgext::MemoryContext*>(CurrentMemoryContext);
    CHECK(ctx != nullptr);

    // Try to reallocate the pointer, if not found do nothing
    return ctx->alloc(size);
}

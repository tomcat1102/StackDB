#include <iostream>
using std::cout;
using std::endl;

#include "arena.h"
using namespace stackdb;

char *Arena::allocate(size_t bytes) {
    assert(bytes < MAX_ALLOC_SIZE);
    if (bytes < alloc_remaining) {
        char *result = alloc_ptr;
        alloc_ptr += bytes;
        alloc_remaining -= bytes;
        return result;
    }
    return allocate_fallback(bytes);
}

char *Arena::allocate_aligned(size_t bytes) {
    constexpr int align = sizeof(void *);
    static_assert((align == 4) | (align == 8), "align must be 4 or 8");

    size_t current_mod = (size_t)alloc_ptr & (align - 1);
    size_t slop = current_mod == 0 ? 0 : align - current_mod;
    size_t needed = bytes + slop;

    char *result;
    if (needed <= alloc_remaining) {
        result = alloc_ptr + slop;
        alloc_ptr += needed;
        alloc_remaining -= needed;
    } else {
        result = allocate_new_block(bytes); // malloc/new guarantees alignment. anyway we do a last check
    }

    assert(((size_t)result & (align - 1)) == 0);
    return result;
}

char* Arena::allocate_fallback(size_t bytes) {
    // avoid wasting too much space in leftover bytes. if alloc 1025 bytes in this 4096,
    // then if next alloc > 3072, then the leftover 3072 bytes in this block are wasted!
    if (bytes > BLOCK_SIZE / 4) {
        return allocate_new_block(bytes);
    }

    alloc_ptr = allocate_new_block(BLOCK_SIZE);
    alloc_remaining = BLOCK_SIZE;
    // alloc on the site now
    char *result = alloc_ptr;
    alloc_ptr += bytes;
    alloc_remaining -= bytes;
    return result;
}

char *Arena::allocate_new_block(size_t block_bytes) {
    char *result = new char[block_bytes];
    blocks.push_back(result);
    // update total memory usage, counting the memory taken by the char* pointer in block vector
    mem_usage.fetch_add(block_bytes + sizeof(char *), std::memory_order_relaxed);
    return result;
}

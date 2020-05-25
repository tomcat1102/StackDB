#ifndef STACKDB_ARENA_H
#define STACKDB_ARENA_H

#include <vector>
#include <atomic>
#include <cassert>

namespace stackdb {
    class Arena {
        const static size_t MAX_ALLOC_SIZE = 1024 * 1024 * 16;
        const static size_t BLOCK_SIZE = 4096;

        public:
            Arena(): alloc_ptr(nullptr), alloc_remaining(0) {}
            Arena(const Arena&) = delete;
            Arena& operator=(const Arena&) = delete;
            ~Arena() {
                for (size_t i = 0, n_blocks = blocks.size(); i < n_blocks; i ++)
                    delete[] blocks[i];
            }

            char *allocate(size_t bytes);
            char *allocate_aligned(size_t bytes);
            // return an estimate of used memory in the arena
            size_t get_mem_usage() const { return mem_usage.load(std::memory_order_relaxed); }

        private:
            char *allocate_fallback(size_t bytes);
            char *allocate_new_block(size_t bytes);

        private:
            char *alloc_ptr;
            size_t alloc_remaining;
            std::vector<char*> blocks;
            std::atomic<size_t> mem_usage;
    };
}

#endif
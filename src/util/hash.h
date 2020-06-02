#ifndef STACKDB_HASH_H
#define STACKDB_HASH_H

#include <cstdint>
#include <cstddef>

// simple hash function used for internal data structures
namespace stackdb {
    uint32_t hash(const char* data, size_t n, uint32_t seed);
}

#endif

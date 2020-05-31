#ifndef STACKDB_CRC32C_H
#define STACKDB_CRC32C_H

#include <cstddef>  // size_t
#include <cstdint>  // uint32_t

namespace stackdb {
namespace crc32c {
    // return crc32 of concat A | data[0, n - 1] where A's crc is represented as cur_crc
    uint32_t extend(uint32_t cur_crc, const char *data, size_t n);
    // return the crc32 of data[0, n - 1]
    inline uint32_t value(const char *data, size_t n) {
        return extend(0, data, n);
    }
    // return a masked representation of a crc
    const uint32_t MASK_DELTA = 0xa282ead8ul;
    inline uint32_t mask(uint32_t crc) {
        return ((crc >> 15) | (crc << 17)) + MASK_DELTA;
    }
    inline uint32_t unmask(uint32_t masked_crc) {
        uint32_t rot = masked_crc - MASK_DELTA;
        return ((rot >> 17) | (rot << 15));
    }
}
}

#endif

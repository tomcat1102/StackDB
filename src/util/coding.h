#ifndef STACKDB_CODING_H
#define STACKDB_CODING_H

#include <string>
#include "stackdb/slice.h"

// Endian-neutral encoding:
//  Fixed-length numbers are encoded with least-significant byte first
//  In addition we support variable length "varint" encoding
//  Strings are encoded prefixed by their length in varint format

namespace stackdb {
    // append coded integers and lengthd slice to dst
    void append_fixed_32(std::string *dst, uint32_t value);
    void append_fixed_64(std::string *dst, uint64_t value);
    void append_varint_32(std::string *dst, uint32_t value);
    void append_varint_64(std::string *dst, uint64_t value);
    void append_length_prefixed_slice(std::string *dst, const Slice &slice);
    // get int and slice from input, advancing parsed input
    bool get_varint_32(Slice *input, uint32_t *value);
    bool get_varint_64(Slice *input, uint64_t *value);
    bool get_length_prefixed_slice(Slice *input, Slice *result);
    // get int from [p, limit - 1] and return ptr past parsed value
    const char *get_varint_32_ptr(const char *p, const char *limit, uint32_t *value);
    const char *get_varint_64_ptr(const char *p, const char *limit, uint64_t *value);
    // return length of varint32 or varint64 encoding of v
    int varint_length(uint64_t v);


    // encode to dst and return ptr past parsed last byte written. dst shall have enough space. for append_varint_xx
    char *encode_varint_32(char *dst, uint32_t value);
    char *encode_varint_64(char *dst, uint64_t value);
    // encode to dst. dst shall have enough space. for append_fixed_xx
    inline void encode_fixed_32(char *dst, uint32_t value) {
        uint8_t *buf = reinterpret_cast<uint8_t*>(dst);
        buf[0] = static_cast<uint8_t>(value >> 0);
        buf[1] = static_cast<uint8_t>(value >> 8);
        buf[2] = static_cast<uint8_t>(value >> 16);
        buf[3] = static_cast<uint8_t>(value >> 24);
    }
    inline void encode_fixed_64(char *dst, uint64_t value) {
        uint8_t *buf = reinterpret_cast<uint8_t*>(dst);
        buf[0] = static_cast<uint8_t>(value >> 0);
        buf[1] = static_cast<uint8_t>(value >> 8);
        buf[2] = static_cast<uint8_t>(value >> 16);
        buf[3] = static_cast<uint8_t>(value >> 24);
        buf[4] = static_cast<uint8_t>(value >> 32);
        buf[5] = static_cast<uint8_t>(value >> 40);
        buf[6] = static_cast<uint8_t>(value >> 48);
        buf[7] = static_cast<uint8_t>(value >> 56);   
    }

    // decode int from ptr without bounds checking.
    inline uint32_t decode_fixed_32(const char *ptr) {
        const uint8_t *buf = reinterpret_cast<const uint8_t*>(ptr);
        return  (static_cast<uint32_t>(buf[0]) << 0 ) |
                (static_cast<uint32_t>(buf[1]) << 8 ) |
                (static_cast<uint32_t>(buf[2]) << 16) |
                (static_cast<uint32_t>(buf[3]) << 24);
    }
    inline uint64_t decode_fixed_64(const char *ptr) {
        const uint8_t *buf = reinterpret_cast<const uint8_t*>(ptr);
        return  (static_cast<uint64_t>(buf[0]) << 0 ) |
                (static_cast<uint64_t>(buf[1]) << 8 ) |
                (static_cast<uint64_t>(buf[2]) << 16) |
                (static_cast<uint64_t>(buf[3]) << 24) |
                (static_cast<uint64_t>(buf[4]) << 32) |
                (static_cast<uint64_t>(buf[5]) << 40) |
                (static_cast<uint64_t>(buf[6]) << 48) |
                (static_cast<uint64_t>(buf[7]) << 56);        
    }

} // namespace stackdb

#endif
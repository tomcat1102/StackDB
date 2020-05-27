#include "util/coding.h"

namespace stackdb {

void append_fixed_32(std::string *dst, uint32_t value) {
    char buf[sizeof(value)];
    encode_fixed_32(buf, value);
    dst->append(buf, sizeof(buf));
}
void append_fixed_64(std::string *dst, uint64_t value) {
    char buf[sizeof(value)];
    encode_fixed_64(buf, value);
    dst->append(buf, sizeof(buf));
}

char *encode_varint_32(char *dst, uint32_t v) {
    uint8_t* ptr = reinterpret_cast<uint8_t*>(dst);
    const int B = 128;
    if (v < (1 << 7)) {
        *(ptr++) = v;
    } else if (v < (1 << 14)) {
        *(ptr++) = v | B;
        *(ptr++) = v >> 7;
    } else if (v < (1 << 21)) {
        *(ptr++) = v | B;
        *(ptr++) = (v >> 7) | B;
        *(ptr++) = v >> 14;
    } else if (v < (1 << 28)) {
        *(ptr++) = v | B;
        *(ptr++) = (v >> 7) | B;
        *(ptr++) = (v >> 14) | B;
        *(ptr++) = v >> 21;
    } else {
        *(ptr++) = v | B;
        *(ptr++) = (v >> 7) | B;
        *(ptr++) = (v >> 14) | B;
        *(ptr++) = (v >> 21) | B;
        *(ptr++) = v >> 28;
    }
    return reinterpret_cast<char*>(ptr);
}
void append_varint_32(std::string* dst, uint32_t value) {
    char buf[5];
    char* ptr = encode_varint_32(buf, value);
    dst->append(buf, ptr - buf);
}

char *encode_varint_64(char *dst, uint64_t v) {
    uint8_t* ptr = reinterpret_cast<uint8_t*>(dst);
    const int B = 128;
    while (v >= B) {
        *(ptr++) = v | B;
        v >>= 7;
    }
    *(ptr++) = static_cast<uint8_t>(v);
    return reinterpret_cast<char*>(ptr);
}
void append_varint_64(std::string* dst, uint64_t value) {
    char buf[10];
    char* ptr = encode_varint_64(buf, value);
    dst->append(buf, ptr - buf);
}

void append_length_prefixed_slice(std::string *dst, const Slice &slice) {
    append_varint_32(dst, slice.size());
    dst->append(slice.data(), slice.size());
}

int varint_length(uint64_t v) {
    int len = 1;
    while (v >= 128) {
        v >>= 7;
        len++;
    }
    return len;
}

const char* get_varint_32_ptr_fallback(const char* p, const char* limit,
                                   uint32_t* value) {
    uint32_t result = 0;
    for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
        uint32_t byte = *(reinterpret_cast<const uint8_t*>(p));
        p++;
        if (byte & 128) { // More bytes are present
            result |= ((byte & 127) << shift);
        } else {
            result |= (byte << shift);
            *value = result;
            return reinterpret_cast<const char*>(p);
        }
    }
    return nullptr;
}

const char* get_varint_32_ptr(const char* p, const char* limit, uint32_t* value) {
    if (p < limit) {
        uint32_t result = *(reinterpret_cast<const uint8_t*>(p));
        if ((result & 128) == 0) {
            *value = result;
            return p + 1;
        }
    }
    return get_varint_32_ptr_fallback(p, limit, value);
}
bool get_varint_32(Slice *input, uint32_t *value) {
    const char* p = input->data();
    const char* limit = p + input->size();
    const char* q = get_varint_32_ptr(p, limit, value);
    if (q == nullptr) {
        return false;
    } else {
        *input = Slice(q, limit - q);
        return true;
    }
}

const char *get_varint_64_ptr(const char *p, const char *limit, uint64_t *value) {
    uint64_t result = 0;
    for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
        uint64_t byte = *(reinterpret_cast<const uint8_t*>(p));
        p++;
        if (byte & 128) { // More bytes are present
            result |= ((byte & 127) << shift);
        } else {
            result |= (byte << shift);
            *value = result;
            return reinterpret_cast<const char*>(p);
        }
    }
    return nullptr;
}
bool get_varint_64(Slice *input, uint64_t *value) {
    const char* p = input->data();
    const char* limit = p + input->size();
    const char* q = get_varint_64_ptr(p, limit, value);
    if (q == nullptr) {
        return false;
    } else {
        *input = Slice(q, limit - q);
        return true;
    }
}

bool get_length_prefixed_slice(Slice *input, Slice *result) {
    uint32_t len;
    if (get_varint_32(input, &len) && input->size() >= len) {
        *result = Slice(input->data(), len);
        input->remove_prefix(len);
        return true;
    } else {
        return false;
    }
}


}
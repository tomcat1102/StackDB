#include <iostream>
#include <vector>
#include <cassert>
#include "util/coding.h"
using namespace stackdb;

int main() {
    // test fixed 32
    {
        std::string s;
        for (uint32_t v = 0; v < 100000; v ++) {
            append_fixed_32(&s, v);
        }

        const char *p = s.data();
        for (uint32_t v = 0; v < 10000; v ++) {
            uint32_t res = decode_fixed_32(p);
            assert(res == v);
            p += sizeof(uint32_t);
        }
    }
    // test fixed 64
    {
        std::string s;
        for (int power = 0; power <= 63; power ++) {
            uint64_t v = static_cast<uint64_t>(1) << power;
            append_fixed_64(&s, v - 1);
            append_fixed_64(&s, v + 0);
            append_fixed_64(&s, v + 1);
        }

        const char *p = s.data();
        for (int power = 0; power <= 63; power ++) {
            uint64_t v = static_cast<uint64_t>(1) << power, res;

            res = decode_fixed_64(p);
            assert(res == v - 1);
            p += sizeof(uint64_t);

            res = decode_fixed_64(p);
            assert(res == v + 0);
            p += sizeof(uint64_t);

            res = decode_fixed_64(p);
            assert(res == v + 1);
            p += sizeof(uint64_t);
        }
    }
    // test endianness
    {
        std::string dst;
        append_fixed_32(&dst, 0x04030201);
        assert(static_cast<int>(dst[0]) == 0x01);
        assert(static_cast<int>(dst[1]) == 0x02);
        assert(static_cast<int>(dst[2]) == 0x03);
        assert(static_cast<int>(dst[3]) == 0x04);

        dst.clear();
        append_fixed_64(&dst, 0x0807060504030201);
        assert(static_cast<int>(dst[0]) == 0x01);
        assert(static_cast<int>(dst[1]) == 0x02);
        assert(static_cast<int>(dst[2]) == 0x03);
        assert(static_cast<int>(dst[3]) == 0x04);
        assert(static_cast<int>(dst[4]) == 0x05);
        assert(static_cast<int>(dst[5]) == 0x06);
        assert(static_cast<int>(dst[6]) == 0x07);
        assert(static_cast<int>(dst[7]) == 0x08);
    }
    // test varint32
    {
        std::string s;
        for (uint32_t i = 0; i < (32 * 32); i++) {
            uint32_t v = (i / 32) << (i % 32);
            append_varint_32(&s, v);
        }

        const char* p = s.data();
        const char* limit = p + s.size();

        for (uint32_t i = 0; i < (32 * 32); i++) {
            uint32_t expected = (i / 32) << (i % 32);
            uint32_t res;
            const char* start = p;
            p = get_varint_32_ptr(p, limit, &res);
            assert(p != nullptr);
            assert(res == expected);
            assert(varint_length(res) == p - start);
        }
        assert(p == s.data() + s.size());
    }
    // teset varint64
    {
        // construct the list of values to check
        std::vector<uint64_t> values;
        // some special values
        values.push_back(0);
        values.push_back(100);
        values.push_back(~static_cast<uint64_t>(0));
        values.push_back(~static_cast<uint64_t>(0) - 1);
        for (uint32_t k = 0; k < 64; k++) {
            // test values near powers of two
            const uint64_t power = 1ull << k;
            values.push_back(power);
            values.push_back(power - 1);
            values.push_back(power + 1);
        }

        std::string s;
        for (size_t i = 0; i < values.size(); i++) {
            append_varint_64(&s, values[i]);
        }

        const char* p = s.data();
        const char* limit = p + s.size();
        for (size_t i = 0; i < values.size(); i++) {
            assert(p < limit);
            uint64_t res;
            const char* start = p;
            p = get_varint_64_ptr(p, limit, &res);
            assert(p != nullptr);
            assert(res == values[i]);
            assert(varint_length(res) == p - start);
        }
        assert(p == limit);
    }
    // test varint32 overflow
    {
        uint32_t result;
        std::string input("\x81\x82\x83\x84\x85\x11");
        assert(get_varint_32_ptr(input.data(), input.data() + input.size(), &result) == nullptr);
    }
    // test varint32 truncation
    {
        uint32_t large_value = (1u << 31) + 100;
        std::string s;
        append_varint_32(&s, large_value);
        uint32_t result;
        for (size_t len = 0; len < s.size() - 1; len++) {
            assert(get_varint_32_ptr(s.data(), s.data() + len, &result) == nullptr);
        }
        assert(get_varint_32_ptr(s.data(), s.data() + s.size(), &result) != nullptr);
        assert(large_value == result);
    }

    // test varint64 overflow)
    {
        uint64_t result;
        std::string input("\x81\x82\x83\x84\x85\x81\x82\x83\x84\x85\x11");
        assert(get_varint_64_ptr(input.data(), input.data() + input.size(), &result) == nullptr);
    }
    // test varint64 truncation 
    {
        uint64_t large_value = (1ull << 63) + 100ull;
        std::string s;
        append_varint_64(&s, large_value);
        uint64_t result;
        for (size_t len = 0; len < s.size() - 1; len++) {
            assert(get_varint_64_ptr(s.data(), s.data() + len, &result) == nullptr);
        }
        assert(get_varint_64_ptr(s.data(), s.data() + s.size(), &result) != nullptr);
        assert(large_value == result);
    }
    // test strings
    {
        std::string s;
        append_length_prefixed_slice(&s, Slice(""));
        append_length_prefixed_slice(&s, Slice("foo"));
        append_length_prefixed_slice(&s, Slice("bar"));
        append_length_prefixed_slice(&s, Slice(std::string(200, 'x')));

        Slice input(s);
        Slice v;
        assert(get_length_prefixed_slice(&input, &v));
        assert(v.to_string() == "");
        assert(get_length_prefixed_slice(&input, &v));
        assert(v.to_string() == "foo");
        assert(get_length_prefixed_slice(&input, &v));
        assert(v.to_string() == "bar");
        assert(get_length_prefixed_slice(&input, &v));
        assert(std::string(200, 'x') == v.to_string());
        assert(input.to_string() == "");
    }

    std::cout << "ok!" << std::endl;
}
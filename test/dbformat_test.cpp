#include <iostream>
#include <string>
#include "db/dbformat.h"
using namespace stackdb;

static std::string internal_key(const std::string &user_key, uint64_t seq, ValType type) {
    std::string encoded;
    append_internal_key(&encoded, ParsedInternalKey(user_key, seq, type));
    return encoded;
}

static std::string shorten(const std::string &start, const std::string &limit) {
    std::string res = start;
    InternalKeyComparator(bytewise_comparator()).find_shortest_separator(&res, limit);
    return res;
}

static std::string short_successor(const std::string &s) {
    std::string res = s;
    InternalKeyComparator(bytewise_comparator()).find_short_successor(&res);
    return res;
}

static void test_key(const std::string &key, uint64_t seq, ValType type) {
    std::string encoded = internal_key(key, seq, type);

    Slice in(encoded);
    ParsedInternalKey decoded("", 0, ValType::VALUE);

    assert(parse_internal_key(in, &decoded));
    assert(decoded.user_key.to_string() == key);
    assert(decoded.seq == seq);
    assert(decoded.type == type);

    assert(!parse_internal_key(Slice("bar"), &decoded));
}

int main() {
    // test internal key encode & decode
    {
        const char* keys[] = {"", "k", "hello", "longggggggggggggggggggggg"};
        const uint64_t seq[] = {1, 2, 3, 
                        (1ull << 8 ) - 1,
                         1ull << 8 ,
                        (1ull << 8 ) + 1,
                        (1ull << 16) - 1,
                         1ull << 16,
                        (1ull << 16) + 1,
                        (1ull << 32) - 1,
                         1ull << 32,
                        (1ull << 32) + 1};
        for (size_t k = 0; k < sizeof(keys) / sizeof(keys[0]); k++) {
            for (size_t s = 0; s < sizeof(seq) / sizeof(seq[0]); s++) {
                test_key(keys[k], seq[s], ValType::VALUE);
                test_key(keys[k], seq[s], ValType::DELETION);   // "hello" ?
            }
        } 
    }
    // test interal key decode from empty
    {
        InternalKey internal_key;
        assert(!internal_key.decode_from(""));
    }
    // test internal key short separator
    {
        // when user keys are same
        assert(internal_key("foo", 100, ValType::VALUE) == 
            shorten(internal_key("foo", 100, ValType::VALUE), internal_key("foo", 99, ValType::VALUE)));
        assert(internal_key("foo", 100, ValType::VALUE) == 
            shorten(internal_key("foo", 100, ValType::VALUE), internal_key("foo", 101, ValType::VALUE)));
        assert(internal_key("foo", 100, ValType::VALUE) == 
            shorten(internal_key("foo", 100, ValType::VALUE), internal_key("foo", 100, ValType::VALUE)));            
        assert(internal_key("foo", 100, ValType::VALUE) == 
            shorten(internal_key("foo", 100, ValType::VALUE), internal_key("foo", 99, ValType::DELETION)));
        // when user keys are misordered
        assert(internal_key("foo", 100, ValType::VALUE) ==
            shorten(internal_key("foo", 100, ValType::VALUE), internal_key("bar", 99, ValType::VALUE)));
        // when user keys are different, but correctly ordered
        assert(internal_key("g", MAX_SEQ_NUM, ValType::SEEK) == 
            shorten(internal_key("foo", 100, ValType::VALUE), internal_key("hello", 200, ValType::VALUE)));
        // when start user key is prefix of limit user key
        assert(internal_key("foo", 100, ValType::VALUE) ==
            shorten(internal_key("foo", 100, ValType::VALUE), internal_key("foobar", 200, ValType::VALUE)));
        // when limit user key is prefix of start user key
        assert(internal_key("foobar", 100, ValType::VALUE) ==
            shorten(internal_key("foobar", 100, ValType::VALUE), internal_key("foo", 200, ValType::VALUE)));
    }
    // test inernal key shortest successor
    {
        assert(internal_key("g", MAX_SEQ_NUM, ValType::SEEK) ==
            short_successor(internal_key("foo", 100, ValType::VALUE)));
        assert(internal_key("\xff\xff", 100, ValType::VALUE) ==
            short_successor(internal_key("\xff\xff", 100, ValType::VALUE)));
    }
    // test parsed internal key debug string
    {
        ParsedInternalKey key("The \"key\" in 'single quotes'", 42, ValType::VALUE);
        assert(key.debug_string() == "'The \"key\" in 'single quotes'' @ 42 : 1");
    }
    // test internal key debug string
    {
        InternalKey key("The \"key\" in 'single quotes'", 42, ValType::VALUE);
        assert(key.debug_string() == "'The \"key\" in 'single quotes'' @ 42 : 1");

        InternalKey invalid_key;
        assert(invalid_key.debug_string() == "(bad)");
    }
    return 0;
}
#ifndef STACKDB_DBFORMAT_H
#define STACKDB_DBFORMAT_H

#include <cstdint>
#include "stackdb/slice.h"
#include "stackdb/comparator.h"

namespace stackdb {
    // different kinds of keys in stackdb
    // 1. user key:  "abc"
    // 2. internal key :
    //      user key | type | sequence num
    //      "abc" | 1 | 1234567..   (type takes 1 lowest byte and seq takes 7 higher bytes)
    // 3. memtable key :
    //      varint for internal key size | internal key
    //      11 | "abc" | 1 | 1234567..
    // 4. lookup key :
    //      helper key, same as memtable key, with a small buffer for short key
    
    enum class ValType{
        DELETION = 0x0,
        VALUE = 0x1,
        SEEK = VALUE
    };

    typedef uint64_t SeqNum;
    const SeqNum MAX_SEQ_NUM = (0x1ull << 56) - 1;   // leave 8 bits at the bottom so type and seq can be packed into 64 bits

    // structure to hold parsed fields in an internal key
    struct ParsedInternalKey {
        Slice user_key;
        SeqNum seq;
        ValType type;

        ParsedInternalKey() {}
        ParsedInternalKey(const Slice& key, const SeqNum& seq, ValType type)
            : user_key(key), seq(seq), type(type) {}

        // _length & 'internal_key_encoding_length()' in leveldb are never used
        size_t _length() const { return user_key.size() + 8; }
        std::string debug_string() const;
    };

    void append_internal_key(std::string *result, const ParsedInternalKey &key);    // append the serialization of key to result
    bool parse_internal_key(const Slice &internal_key, ParsedInternalKey *result);  // parse internal key. result undefined if false returned
    inline Slice extract_user_key(const Slice& internal_key) {                      // extract user_key field from a internal_key
        assert(internal_key.size() >= 8);
        return Slice(internal_key.data(), internal_key.size() - 8);
    }


    class InternalKey {
    public:
        InternalKey() {} // empty rep indicates invalid internal key
        InternalKey(const Slice& user_key, SeqNum seq, ValType type) {
            append_internal_key(&rep, ParsedInternalKey(user_key, seq, type));
        }
        // decode from slice to this internal key
        bool decode_from(const Slice& s) {
            rep.assign(s.data(), s.size());
            return !rep.empty();
        }
        // encode this internal key to slice
        Slice encode() const {
            assert(!rep.empty());
            return rep;     // implicit conversion
        }
        Slice user_key() const { return extract_user_key(rep); }

        void set_from(const ParsedInternalKey& key) {
            rep.clear();
            append_internal_key(&rep, key);
        }
        void clear() { rep.clear(); };
        std::string debug_string() const;

    private:
        std::string rep;
    };

    class InternalKeyComparator: public Comparator {
    public:
        explicit InternalKeyComparator(const Comparator* cmp): user_cmp(cmp) {}

        int compare(const InternalKey &a, const InternalKey &b) const {
            return compare(a.encode(), b.encode());
        }
        const Comparator *user_comparator() const { return user_cmp; };

        // Comparator interfaces
        const char *name() const override;
        int compare(const Slice &a, const Slice &b) const override;
        void find_shortest_separator(std::string *start, const Slice &limit) const override;
        void find_short_successor(std::string *key) const override;

    private:
        const Comparator *user_cmp;
    };

    // helper class for DBImpl::get(). used to look up key at a snopshot with specified seq. constructed as:
    //    length    varint32            <-- start_
    //    user_key  char[klength]       <-- key_start
    //    seq_num   uint64
    //                                  <-- end
    // The array is a suitable MemTable key. The suffix starting with "userkey" can be used as an InternalKey.
    class LookupKey {
    public:
        LookupKey(const Slice &user_key, SeqNum seq);
        LookupKey(const LookupKey &) = delete;
        ~LookupKey() { 
            if (start != space) delete[] start;
        }
        LookupKey& operator=(const LookupKey&) = delete;



    private:
        const char *start;      // may point to "space" or new allocated mem
        const char *key_start;
        const char *end;
        char space[200];        // avoid allocation for short keys
    };
} // namespace stackdb

#endif
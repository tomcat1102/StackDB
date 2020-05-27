#include <sstream>
#include "db/dbformat.h"
#include "util/logging.h"
#include "util/coding.h"

namespace stackdb {

uint64_t pack_seq_and_type(uint64_t seq, ValType type) {
    assert(seq <= MAX_SEQ_NUM);
    assert(type <= ValType::SEEK);
    uint64_t _type = static_cast<int>(type);
    return (seq << 8) | _type;
}
void append_internal_key(std::string *result, const ParsedInternalKey &key) {
    result->append(key.user_key.data(), key.user_key.size());
    append_fixed_64(result, pack_seq_and_type(key.seq, key.type));
}
bool parse_internal_key(const Slice& internal_key, ParsedInternalKey *result) {
    // check minimal length
    size_t n = internal_key.size();
    if (n < 8) return false;

    // check value type
    uint64_t seq_type = decode_fixed_64(internal_key.data() + n - 8);
    ValType type = static_cast<ValType>(seq_type & 0xff);
    if (type > ValType::SEEK) return false;

    result->user_key = Slice(internal_key.data(), n - 8);
    result->seq = seq_type >> 8;
    result->type = type;
    return true;
}

std::string ParsedInternalKey::debug_string() const {
    std::ostringstream ss;
    // 'user_key' @ sequence : 1
    ss << '\'' << escape_string(user_key.to_string()) << 
        "' @ " << seq << " : " << static_cast<int>(type);
    return ss.str();
}
std::string InternalKey::debug_string() const {
    ParsedInternalKey parsed;
    if (parse_internal_key(rep, &parsed)) {
        return parsed.debug_string();
    }

    std::ostringstream ss;
    ss << "(bad)" << escape_string(rep);
    return ss.str();
}

// implementation for Comparator interface
const char *InternalKeyComparator::name() const {
    return "stackdb.InternalKeyComparator";
}
// Order by:
//    increasing user key (according to user-supplied comparator)
//    decreasing sequence number
//    decreasing type (though sequence# should be enough to disambiguate)
int InternalKeyComparator::compare(const Slice& a_key, const Slice& b_key) const {
    int res = user_cmp->compare(extract_user_key(a_key), extract_user_key(b_key));
    if (res == 0) {
        uint64_t a_seq_type = decode_fixed_64(a_key.data() + a_key.size() - 8);
        uint64_t b_seq_type = decode_fixed_64(b_key.data() + b_key.size() - 8);
        if (a_seq_type > b_seq_type) {          // a_key is newer, thus a is smaller
            return -1;
        } else if (a_seq_type < b_seq_type) {   // b_key is newer, thus a is bigger
            return +1;
        }
    }
    return res;
}
void InternalKeyComparator::find_shortest_separator(std::string *start, const Slice &limit) const {
    Slice user_start = extract_user_key(*start);
    Slice user_limit = extract_user_key(limit);

    std::string tmp(user_start.data(), user_start.size());
    user_cmp->find_shortest_separator(&tmp, user_limit);
    // update start with tmp, if tmp is really smaller than start in size and compare
    if ((tmp.size() < user_start.size()) & (user_cmp->compare(user_start, tmp) < 0)) {
        append_fixed_64(&tmp, pack_seq_and_type(MAX_SEQ_NUM, ValType::SEEK));
        assert(this->compare(*start, tmp) < 0);
        assert(this->compare(tmp, limit) < 0);
        start->swap(tmp);
    }
}
void InternalKeyComparator::find_short_successor(std::string *key) const {
    Slice user_key = extract_user_key(*key);

    std::string tmp(user_key.data(), user_key.size());
    user_cmp->find_short_successor(&tmp);
    // update key with tmp, if tmp is really smaller than start in size and compare
    if ((tmp.size() < user_key.size()) && (user_cmp->compare(user_key, tmp) < 0)) {
        append_fixed_64(&tmp, pack_seq_and_type(MAX_SEQ_NUM, ValType::SEEK));
        assert(this->compare(*key, tmp) < 0);
        key->swap(tmp);
    }
}

LookupKey::LookupKey(const Slice &user_key, SeqNum seq) {
    size_t usize = user_key.size();
    size_t needed = usize + 13;     // estimate. 4=varint32 + usize + 8=seq_num

    char *dst;
    if (needed <= sizeof(space)) { 
        dst = space;
    } else {
        dst = new char[needed];
    }

    start = dst;
    dst = encode_varint_32(dst, usize + 8);
    key_start = dst;
    std::memcpy(dst, user_key.data(), usize);
    dst += usize;
    encode_fixed_64(dst, pack_seq_and_type(seq, ValType::SEEK));
    dst += 8;
    end = dst;
}

} // namespace stackdb
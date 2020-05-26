#include <limits>           // not <climits>, they are different
#include "util/logging.h"

// must wrap with namespace or prefix func names with stackdb::, otherwise we are defining 
// functions in global scope instead of in stackdb, leading to ambiguous failed compilation
namespace stackdb {
    // may be it is faster to use snprintf than std::stringstream
    void append_number_to(std::string *str, uint64_t num) {
        char buf[30];
        snprintf(buf, sizeof(buf), "%llu", (unsigned long long)num);
        str->append(buf);
    }
    void append_escaped_string_to(std::string *str, const Slice &value) {
        char buf[10];
        for (size_t i = 0; i < value.size(); i ++) {
            char c = value[i];
            if (c >= ' ' && c <= '~') {             // 32 ~ 126
                str->push_back(c);
            } else {
                snprintf(buf, sizeof(buf), "\\x%02x",       // hope it's right
                    static_cast<unsigned int>(c) & 0xff);
            }
        }
    }

    std::string number_to_string(uint64_t num) {
        std::string str;
        append_number_to(&str, num);
        return str;
    }
    std::string escape_string(const Slice &value) {
        std::string str;
        append_escaped_string_to(&str, value);
        return str;
    }

    bool consume_decimal_number(Slice *in, uint64_t *val) {
        constexpr uint64_t MAX_UINT_64 = std::numeric_limits<uint64_t>::max();
        constexpr uint8_t MAX_UINT_64_LAST_DIGIT = '0' + MAX_UINT_64 % 10;
        // cast out signedness in char
        const uint8_t *beg = reinterpret_cast<const uint8_t*>(in->data());
        const uint8_t *cur = beg;
        size_t size = in->size();
        uint64_t value = 0;

        while (size != 0) {
            uint8_t ch = *cur;
            if (ch < '0' || ch > '9') break;
            // pre-detect possible overflow
            if (value >  MAX_UINT_64 / 10 || 
            (value == MAX_UINT_64 / 10 && ch > MAX_UINT_64_LAST_DIGIT)) {
                return false;
            }
            cur++;
            size--;

            value = (value * 10) + (ch - '0');
        }

        *val = value;
        in->remove_prefix(cur - beg);   // advance slice
        return cur != beg;              // true if at least one char parsed
    }
}
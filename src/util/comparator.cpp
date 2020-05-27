#include "stackdb/comparator.h"
#include "stackdb/slice.h"

namespace stackdb {
    namespace { // anonymous namespace to hide BytewiseComparator and its singleton
        class BytewiseComparator: public Comparator {
            const char *name() const override { 
                return "stackbd.BytewiseComparator";
            }
            int compare(const Slice &a, const Slice &b) const override {
                return a.compare(b);
            }
            void find_shortest_separator(std::string *start, const Slice& limit) const override {
                size_t min_length = std::min(start->size(), limit.size());
                size_t diff_index = 0;
                
                while (diff_index < min_length && (*start)[diff_index] == limit[diff_index])
                    diff_index++;
                // consider shortening only if *start is not a prefix of limit
                if (diff_index < min_length) {
                    uint8_t diff_byte = (*start)[diff_index];
                    if (diff_byte < 0xff && diff_byte + 1 < limit[diff_index]) {
                        (*start)[diff_index] ++;
                        start->resize(diff_index + 1);
                        assert(compare(*start, limit) < 0);
                    }
                }
            }
            void find_short_successor(std::string *key) const override {
                // find the first incrementable character
                size_t n = key->size();
                for (size_t i = 0; i < n; i ++) {
                    if ((*key)[i] != 0xff) {
                        (*key)[i] ++;
                        key->resize(i + 1);
                        return;
                    }
                }
            }
        } singleton;
    }

    const Comparator *bytewise_comparator() {
        return &singleton;
    }

} // namespace stackdb
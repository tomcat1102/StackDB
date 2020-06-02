#include "stackdb/filter_policy.h"
#include "stackdb/slice.h"
#include "util/hash.h"

namespace stackdb {
    static uint32_t bloom_hash(const Slice& key) {
        return hash(key.data(), key.size(), 0xbc9f1d34);
    }

    class BloomFilterPolicy: public FilterPolicy {
    public:
        explicit BloomFilterPolicy(int bits_per_key) : bits_per_key(bits_per_key) {
            k = bits_per_key * 0.69;
            if (k < 1) k = 1;
            if (k > 30) k = 30;
        }
        // implementation
        const char *name() const override { return "stackdb.BuiltinBloomFilter"; }
        void create_filter(const Slice *keys, int n, std::string *dst) const override {
            // compute bloom filter size (in both bits and bytes)
            size_t bits = n * bits_per_key;
            if (bits < 64) bits = 64;       // avoid high false positive rate for small n

            size_t bytes = (bits + 7) / 8;
            bits = bytes * 8;

            size_t init_size = dst->size();
            dst->resize(init_size + bytes, 0);
            dst->push_back(static_cast<char>(k));               // record num of probes
            
            char* array = &(*dst)[init_size];

            for (int i = 0; i < n; i++) {                       // for each key
                uint32_t h = bloom_hash(keys[i]);
                uint32_t delta = (h >> 17) | (h << 15);         // rotate right 17 bits

                for (size_t j = 0; j < k; j++) {                // for each probe of key
                    uint32_t bitpos = h % bits;
                    array[bitpos / 8] |= (1 << (bitpos % 8));
                    h += delta;
                }
            }
        }

        bool key_may_match(const Slice& key, const Slice& bloom_filter) const override {
            size_t len = bloom_filter.size();
            size_t bits = (len - 1) * 8;
            if (len < 2) return false;                  // however, min len as in create_filter() is 9

            const char* array = bloom_filter.data();
            const size_t k = array[len - 1];
            if (k > 30) return true;                    // reserved 

            uint32_t h = bloom_hash(key);
            uint32_t delta = (h >> 17) | (h << 15);     // Rotate right 17 bits

            for (size_t j = 0; j < k; j++) {
                uint32_t bitpos = h % bits;
                if ((array[bitpos / 8] & (1 << (bitpos % 8))) == 0) return false;
                h += delta;
            }
            return true;
        }
    private:
        size_t bits_per_key;
        size_t k;   // num of probes, less than bits_per_key to reduce probing cost
    };

    const FilterPolicy* new_bloom_filter_policy(int bits_per_key) {
        return new BloomFilterPolicy(bits_per_key);
    }
} // namespace stackdb

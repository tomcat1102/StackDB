#include <vector>
#include "stackdb/filter_policy.h"
#include "stackdb/slice.h"
#include "util/coding.h"
using namespace stackdb;

static int verbose = 0; // control test output

static Slice key(int i, char *buffer) {
    encode_fixed_32(buffer, i);
    return Slice(buffer, sizeof(int));
}
static int next_length(int length) {
    if (length < 10) {
        length += 1;
    } else if (length < 100) {
        length += 10;
    } else if (length < 1000) {
        length += 100;
    } else {
        length += 1000;
    }
    return length;
}

class BloomTest {
public:
    BloomTest(): policy(new_bloom_filter_policy(10)) {}
    ~BloomTest() { delete policy; }

    void reset() {
        keys.clear();
        filter.clear();
    }

    void add(const Slice &s) { keys.push_back(s.to_string()); }
    
    void build() {
        std::vector<Slice> key_slices;
        for (size_t i = 0; i < keys.size(); i ++) {
            key_slices.push_back(Slice(keys[i]));
        }
        filter.clear();
        policy->create_filter(&key_slices[0], key_slices.size(), &filter);
        keys.clear();
    }
    
    size_t filter_size() const { return filter.size(); }
    
    bool matches(const Slice &s) {
        if (!keys.empty()) {
            build();
        }
        return policy->key_may_match(s, filter);
    }

    double false_positive_rate() {
        char buffer[sizeof(int)];
        int result = 0;
        for (int i = 0; i < 10000; i++) {
            if (matches(key(i + 1000000000, buffer))) {
                result++;
            }
        }
        return result / 10000.0;
    }

private:
    const FilterPolicy *policy;
    std::string filter;
    std::vector<std::string> keys;
};

int main() {
    // test empty filter
    {
        BloomTest bloom;
        assert(!bloom.matches("hello"));
        assert(!bloom.matches("world"));
    }
    // test small
    {
        BloomTest bloom;
        bloom.add("hello");
        bloom.add("world");
        assert(bloom.matches("hello"));
        assert(bloom.matches("world"));
        assert(!bloom.matches("x"));
        assert(!bloom.matches("foo"));
    }
    // test varying lengths
    {
        BloomTest bloom;
        char buffer[sizeof(int)];

        // counter num filter that exceeds false positive rate
        int mediocre_filters = 0;
        int good_filters = 0;

        for (int length = 1; length <= 10000; length = next_length(length)) {
            bloom.reset();
            for (int i = 0; i < length; i ++) {
                bloom.add(key(i, buffer));
            }
            bloom.build();

            // check filter size
            assert(bloom.filter_size() <= (size_t)(length * 10 / 8 + 40));
            // added keys must match
            for (int i = 0; i < length; i++) {
                assert(bloom.matches(key(i, buffer)));
            }
            // check false positive rate
            double rate = bloom.false_positive_rate();
            if (verbose >= 1) {
                fprintf(stderr, "False positives: %5.2f%% @ length = %6d ; bytes = %6d\n",
                        rate * 100.0, length, (int)bloom.filter_size());
            }
            assert(rate <= 0.02);   // must not be over 2%
            if (rate > 0.0125)
                mediocre_filters++; // allowed, but not too often
            else
                good_filters++;
            }
            if (verbose >= 1) {
                fprintf(stderr, "Filters: %d good, %d mediocre\n", good_filters, mediocre_filters);
            }
            assert(mediocre_filters <= good_filters / 5);
    }
    return 0;
}

#ifndef STACKDB_FILTER_POLICY_H
#define STACKDB_FILTER_POLICY_H

#include <string>

// A database can be configured with a custom FilterPolicy object, which is 
// responsible for creating from keys a small filter. filters are stored and 
// consulted to decide whether to read info from disk. could cut down number 
// of disk seeks from a handful to a single disk seek per DB::Get() call.

namespace stackdb {
    class Slice;    // forward declaration

    class FilterPolicy {
    public:
        virtual ~FilterPolicy();
        // interfaces
        // return policy name
        virtual const char *name() const = 0;       
        // append a filter that summarizes orders keys[0, n - 1] to *dst
        virtual void create_filter(const Slice *keys, int n, std::string *dst) const = 0;
        // must return true if key was fiterred in create_filter().
        // may return true or false if key is not filtered, should aim to return false with a high probability.
        virtual bool key_may_match(const Slice& key, const Slice& filter) const = 0;
    };

    // return a new filter policy that uses a bloom filter with approximately
    // specified number of bits per key. A good value for bits_per_key is 10,
    // which yields a filter with ~ 1% false positive rate.
    const FilterPolicy *new_bloom_filter_policy(int bits_per_key);
} // namespace stackdb

#endif

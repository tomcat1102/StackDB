#include "table/filter_block.h"
#include "stackdb/filter_policy.h"
#include "util/coding.h"

namespace stackdb {
    // generate filter for every 2KB of data
    const size_t FILTER_BASE_LOG = 11;
    const size_t FILTER_BASE = 1 << FILTER_BASE_LOG;

    void FilterBlockBuilder::start_block(uint64_t block_offset) {
        uint64_t filter_index = block_offset / FILTER_BASE;
        assert(filter_index >= filter_offsets.size());
        while (filter_index > filter_offsets.size()) {
            generate_filter();
        }
    }

    void FilterBlockBuilder::add_key(const Slice &key) {
        start.push_back(keys.size());
        keys.append(key.data(), key.size());
    }

    Slice FilterBlockBuilder::finish() {
        if (!start.empty()) {
            generate_filter();
        }

        // append filter offset array
        uint32_t array_offset = result.size();
        size_t num_filters = filter_offsets.size();
        for (size_t i = 0; i < num_filters; i ++) {
            append_fixed_32(&result, filter_offsets[i]);
        }

        // append start of filter offsets & base log
        append_fixed_32(&result, array_offset);
        result.push_back(FILTER_BASE_LOG);

        return Slice(result);
    }

    void FilterBlockBuilder::generate_filter() {
        size_t num_keys = start.size();
        if (num_keys == 0) {        // fast path if no keys for this filter
            filter_offsets.push_back(result.size());
            return;
        }

        start.push_back(keys.size());   // for start[i + 1] at last iteration
        tmp_keys.resize(num_keys);      // reuse tmp_keys for crete_filter()
        for (size_t i = 0; i < num_keys; i ++) {
            const char *base = keys.data() + start[i];
            size_t length = start[i + 1] - start[i];
            tmp_keys[i] = Slice(base, length);
        }

        // generate new filter for current keys and append to result
        filter_offsets.push_back(result.size());    // record new filter offset
        policy->create_filter(&tmp_keys[0], num_keys, &result);

        // clear out
        keys.clear();
        start.clear();
        tmp_keys.clear();   // no need, actually...
    }

    FilterBlockReader::FilterBlockReader(const FilterPolicy* policy, const Slice& contents)
        : policy(policy), data(nullptr), offset(nullptr), num(0), base_log(0) {
        size_t n = contents.size();
        if (n < 5) return;              // 1 for base_log and 4 for start of filter offsets

        base_log = contents[n - 1];
        uint32_t filter_data_end = decode_fixed_32(contents.data() + n - 5);
        if (filter_data_end > n - 5) return;

        data = contents.data();
        offset = contents.data() + filter_data_end;
        num = (n - 5 - filter_data_end) / 4;
    }

    bool FilterBlockReader::key_may_match(uint64_t block_offset, const Slice& key) {
        uint64_t filter_index = block_offset >> base_log;
        if (filter_index < num) {
            uint32_t start = decode_fixed_32(offset + filter_index * 4);
            uint32_t limit = decode_fixed_32(offset + filter_index * 4 + 4);

            if (start < limit && limit <= static_cast<size_t>(offset - data)) {
                Slice filter = Slice(data + start, limit - start);
                return policy->key_may_match(key, filter);
            } else if (start == limit) {
                return false;           // empty filters do not match any keys
            }
        }
        return true;   // treat missing and last filter case as potential match
    }
}


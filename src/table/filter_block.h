#ifndef STACKDB_FILTER_BLOCK_H
#define STACKDB_FILTER_BLOCK_H

#include <vector>
#include <cstdint>
#include "stackdb/slice.h"

// a single filter block is stored near a table file end. 
// contains (bloom) filters for each table data block
namespace stackdb {
    class FilterPolicy;
    // FilterBlockBuilder constructs all filters for a particular Table.
    // It generates a single string stored as a filter block in the Table.
    //
    // The sequence of calls to FilterBlockBuilder must match the regexp:
    //      (start_block add_key*)* finish
    class FilterBlockBuilder {
    public:
        explicit FilterBlockBuilder(const FilterPolicy* policy)
            : policy(policy) {}

        FilterBlockBuilder(const FilterBlockBuilder&) = delete;
        FilterBlockBuilder& operator=(const FilterBlockBuilder&) = delete;

        void start_block(uint64_t block_offset);
        void add_key(const Slice& key);
        Slice finish();
    private:
        void generate_filter();

        const FilterPolicy  *policy;
        std::string keys;                       // flattened key contents for current filter
        std::vector<size_t> start;              // starting index of each key in flattened
        std::string result;                     // filter data computed so far
        std::vector<Slice> tmp_keys;            // policy->create_filter() argument
        std::vector<uint32_t> filter_offsets;
    };

    class FilterBlockReader {
    public:
        // REQUIRES: "contents" and *policy must stay live while *this is live.
        FilterBlockReader(const FilterPolicy* policy, const Slice& contents);
        bool key_may_match(uint64_t block_offset, const Slice& key);

    private:
    const FilterPolicy* policy;
        const char* data;       // pointer to start of filter data = start of filter block
        const char* offset;     // pointer to end of filter data = start of filter offset array
        size_t num;             // num of entries in filter offset array
        size_t base_log;        // encoding parameter (see FILTER_BASE_LOG in .cc file)
    };
} // namespace stackdb

#endif


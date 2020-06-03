#include "stackdb/filter_policy.h"
#include "table/filter_block.h"
#include "util/hash.h"
#include "util/coding.h"
#include "util/logging.h"
using namespace stackdb;

// for testing: emit an array with one hash value per key
class TestHashFilter : public FilterPolicy {
    public:
        const char* name() const override { return "TestHashFilter"; }

        void create_filter(const Slice* keys, int n, std::string* dst) const override {
            for (int i = 0; i < n; i++) {
                uint32_t h = hash(keys[i].data(), keys[i].size(), 1);
                append_fixed_32(dst, h);
            }
        }

        bool key_may_match(const Slice& key, const Slice& filter) const override {
            uint32_t h = hash(key.data(), key.size(), 1);
            for (size_t i = 0; i + 4 <= filter.size(); i += 4) {
                if (h == decode_fixed_32(filter.data() + i))
                    return true;
            }
            return false;
        }
};

int main() {
    // test empty builder
    {
        TestHashFilter filter;
        FilterBlockBuilder builder(&filter);

        Slice block = builder.finish();
        assert(escape_string(block) == "\\x00\\x00\\x00\\x00\\x0b");
        FilterBlockReader reader(&filter, block);
        assert(reader.key_may_match(0, "foo"));
        assert(reader.key_may_match(100000, "foo"));
    }
    // test single chunk
    {
        TestHashFilter filter;
        FilterBlockBuilder builder(&filter);

        builder.start_block(100);
        builder.add_key("foo");
        builder.add_key("bar");
        builder.add_key("box");
        builder.start_block(200);
        builder.add_key("box");
        builder.start_block(300);
        builder.add_key("hello");
        Slice block = builder.finish();
        FilterBlockReader reader(&filter, block);
        assert(reader.key_may_match(100, "foo"));
        assert(reader.key_may_match(100, "bar"));
        assert(reader.key_may_match(100, "box"));
        assert(reader.key_may_match(200, "foo"));
        assert(reader.key_may_match(300, "hello"));
        assert(!reader.key_may_match(100, "missing"));
        assert(!reader.key_may_match(100, "other"));
    }
    // test multi chunks
    {
        TestHashFilter filter;
        FilterBlockBuilder builder(&filter);

        // first filter
        builder.start_block(0);
        builder.add_key("foo");
        builder.start_block(2000);
        builder.add_key("bar");

        // second filter
        builder.start_block(3100);
        builder.add_key("box");

        // Third filter is empty

        // Last filter
        builder.start_block(9000);
        builder.add_key("box");
        builder.add_key("hello");

        Slice block = builder.finish();
        FilterBlockReader reader(&filter, block);

        // check first filter
        assert(reader.key_may_match(0, "foo"));
        assert(reader.key_may_match(2000, "bar"));
        assert(!reader.key_may_match(0, "box"));
        assert(!reader.key_may_match(0, "hello"));

        // check second filter
        assert(reader.key_may_match(3100, "box"));
        assert(!reader.key_may_match(3100, "foo"));
        assert(!reader.key_may_match(3100, "bar"));
        assert(!reader.key_may_match(3100, "hello"));

        // check third filter (empty)
        assert(!reader.key_may_match(4100, "foo"));
        assert(!reader.key_may_match(4100, "bar"));
        assert(!reader.key_may_match(4100, "box"));
        assert(!reader.key_may_match(4100, "hello"));

        // check last filter
        assert(reader.key_may_match(9000, "box"));
        assert(reader.key_may_match(9000, "hello"));
        assert(!reader.key_may_match(9000, "foo"));
        assert(!reader.key_may_match(9000, "bar"));
    }
}


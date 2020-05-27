#ifndef STACKDB_COMPARATOR_H
#define STACKDB_COMPARATOR_H

#include <string>

namespace stackdb {
    class Slice;        // mimimum header dependency. We don't #include slice.h

    class Comparator {
    public:
        virtual ~Comparator() = default;
        // three-way comparision
        virtual int compare(const Slice &a, const Slice &b) const = 0;
        // name of the comparator. used to avoid misuse for different dbs. "stackdb.*" are reserved
        virtual const char *name() const = 0;
        // If *start < limit, find a separator so that *start < separator < limit. return the separator via *start
        // e.g for "helloworld" and "hellozoomer", may return "hellox"
        virtual void find_shortest_separator(std::string *start, const Slice &limit) const = 0;
        // change *key to a short string >= *key
        // e.g for "aaaaabbbbbcccc", may return "b", seems not very usefull
        virtual void find_short_successor(std::string *key) const = 0;
    };

    // return a builtin comparator that compare in a lexicographic bytewise order
    const Comparator *bytewise_comparator();
} // namespace stackdb

#endif

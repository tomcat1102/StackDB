#ifndef STACKDB_MEMTABLE_H
#define STACKDB_MEMTABLE_H

#include "db/dbformat.h"
#include "db/skiplist.h"
#include "stackdb/iterator.h"
#include "util/arena.h"

namespace stackdb {
    // MemTables are reference counted. init ref is 0 so caller must call ref() at least once.
    //                                                  memtable key | memtable value
    //
    //               varint size | user key | type | sequence num                       varint size | value
    // memtable key:          11 | "abc"    | 1    | 1234567            memtable value:           3 | "yes"
    class MemTable {
    public:
        explicit MemTable(const InternalKeyComparator &cmp)
            : comparator(cmp), table(cmp, &arena), refs(0) {}
        MemTable(const MemTable &) = delete;
        MemTable& operator=(const MemTable&) = delete;

        // reference counting
        void ref() { ++ refs; }
        void unref() {
            -- refs;
            assert(refs >= 0);
            if (refs <= 0){
                delete this;    // MemTable objects are allocated in free store, so delete ptr to it             
            }
        }
        // approximate memory usage in bytes
        size_t approxi_mem_usage() { return arena.get_mem_usage(); }
        // iterator over the memtable. live while the memtable is live.
        // key() returned by this iterator are internal keys encoded by append_internal_key()
        Iterator *new_iterator();
        // add an entry into memtable that maps user_key to value at specified seq num.
        // typically value will be empty if type == DELETETION
        void add(SeqNum seq, ValType type, const Slice &key, const Slice &value);
        // if contains a value for key, store it in *value and return true. 
        // if contains a deletion for key, store a NotFound() error in *status and return true.
        // else return false.
        bool get(const LookupKey &key, std::string *value, Status *s);

        friend class MemTableIterator;
    private:
        // private deconstructor. so MemTable object can only be allocated on head, not on stack
        ~MemTable() { assert(refs == 0); }
        // wrapper for InternalKeyComparator, define operator()(a, b) since SkipList invokes compare(a, b)
        struct KeyComparator {
            KeyComparator(const InternalKeyComparator &cmp) : comparator(cmp) {} // BUG: remove 'explicit' or MemTable() won't compile
            int operator()(const char *a, const char *b) const;
            const InternalKeyComparator comparator;
        };
        typedef SkipList<const char *, KeyComparator> Table;

        KeyComparator comparator;
        Arena arena;
        Table table;
        int refs;
    };
}

#endif
#ifndef STACKDB_SKIPLIST_H
#define STACKDB_SKIPLIST_H

#include <atomic>
#include "util/arena.h"
#include "util/random.h"

// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.

namespace stackdb {
    // SkipList
    // New SkipList object will use "cmp" for comparing keys,
    // and will allocate memory using "*arena".  Objects allocated in the arena
    // must remain allocated for the lifetime of the skiplist object.
    template<typename Key, class Comparator>
    class SkipList {
        private: struct Node;    // internal node that makes up the list        
        public:  class Iterator; // class that iterates over contents of a skip list.

    public:
        explicit SkipList(Comparator cmp, Arena *arena);
        SkipList(const SkipList&) = delete;

        SkipList& operator=(const SkipList&) = delete;

        void insert(const Key &key);            // insert key into the list. key mustn't already in the list
        bool contains(const Key &key) const;    // return true iff an entry that compares eqaul to key is in the list

    private:
        const static int MAX_HEIGHT = 12;
        int get_max_height() const {                // return list max height
            return max_height.load(std::memory_order_relaxed);
        } 
        int random_height();                        

        bool equal(const Key &a, const Key &b) const {
            return compare(a, b) == 0;
        }
        bool key_is_after_node(const Key &key, Node *n) const {
            return (n != nullptr) && (compare(n->key, key) < 0);
        }

        Node *new_node(const Key &key, int height);                     // return earliest node that comes at or after key, and 
        Node *find_greater_or_equal(const Key &key, Node **prev) const; //  fill prev for every level in [0, max_height - 1]
        Node *find_less_than(const Key& key) const;                     // return the latested node with k < key or head
        Node *find_last() const;                                        // return the last (rightest and lowest) node, head if empty

        // immuatable after construction
        const Comparator compare;
        Arena *const arena;
        Node *const head;
        std::atomic<int> max_height;
        Random rnd;
    };

    template<typename Key, class Comparator>
    void SkipList<Key, Comparator>::insert(const Key& key) {
        Node *prev[MAX_HEIGHT];
        Node *node = find_greater_or_equal(key, prev);
        // check no duplicate before insertion
        assert(node == nullptr || !equal(node->key, key));

        // random height for the new node, update max_height if needed
        int height = random_height();
        if (height > get_max_height()) {
            for (int i = get_max_height(); i < height; i ++) {
                prev[i] = head;
            }
            max_height.store(height, std::memory_order_relaxed);
        }

        node = new_node(key, height);
        for (int i = 0; i < height; i ++) {
            node->no_barrier_set_next(i, prev[i]->no_barrier_next(i));
            prev[i]->set_next(i, node);
        }
    }

    template<typename Key, class Comparator>
    bool SkipList<Key, Comparator>::contains(const Key& key) const {
        Node *node = find_greater_or_equal(key, nullptr);
        return node != nullptr && equal(node->key, key);
    }

    template<typename Key, class Comparator>
    SkipList<Key, Comparator>::SkipList(Comparator cmp, Arena *arena)
        :compare(cmp), arena(arena), head(new_node(0, MAX_HEIGHT)), max_height(1), rnd(0xdeadbeef) {
        for (int i = 0; i < MAX_HEIGHT; i ++)
            head->set_next(i, nullptr);
    }

    template<typename Key, class Comparator>
    int SkipList<Key, Comparator>::random_height() {
        int branching = 4, height = 1;
        while (height < MAX_HEIGHT && ((rnd.next() % branching) == 0))
            height++;
        assert(height > 0);
        assert(height <= MAX_HEIGHT);
        return height;
    }

    template<typename Key, class Comparator>
    typename SkipList<Key, Comparator>::Node *
    SkipList<Key, Comparator>::new_node(const Key& key, int height) {
        char *const node_memory = arena->allocate_aligned(
            sizeof(Node) + sizeof(std::atomic<Node *>) * (height - 1));
        return new (node_memory) Node(key); // replacement new
    }

    template<typename Key, class Comparator>
    typename SkipList<Key, Comparator>::Node *
    SkipList<Key, Comparator>::find_greater_or_equal(const Key& key, Node **prev) const {
        int level = get_max_height() - 1;
        Node *cur = head, *next;
        while (true) {
            next = cur->next(level);
            if (key_is_after_node(key, next)) { // keep search at this level
                cur = next;
            } else {
                if (prev != nullptr) prev[level] = cur;
                if (level == 0) {
                    return next;
                } else {        // switch to next level
                    level--;
                }
            }
        }
    }

    template<typename Key, class Comparator>
    typename SkipList<Key, Comparator>::Node *
    SkipList<Key, Comparator>::find_less_than(const Key& key) const {
        int level = get_max_height() - 1;
        Node *cur = head, *next;
        while (true) {
            assert(cur == head || compare(cur->key, key) < 0);
            next = cur->next(level);
            if (next == nullptr || compare(next->key, key) >= 0) {
                if (level == 0) {
                    return cur;
                } else {        // switch to next level
                    level--;
                }
            } else {
                cur = next;
            }
        }
    }

    template<typename Key, class Comparator>
    typename SkipList<Key, Comparator>::Node *
    SkipList<Key, Comparator>::find_last() const {
        int level = get_max_height() - 1;
        Node *cur = head, *next;
        while (true) {
            next = cur->next(level);
            if (next == nullptr) {
                if (level == 0) {
                    return cur;
                } else {
                    level--;    // switch to next level
                }
            } else {
                cur = next;
            }
        }
    }

    // SkipList::Node
    template<typename Key, class Comparator>
    struct SkipList<Key, Comparator>::Node {
        explicit Node(const Key& k): key(k) {}
        
        Node *next(int n)                        { assert(n >= 0); return nexts[n].load(std::memory_order_acquire); }
        void set_next(int n, Node *x)            { assert(n >= 0); nexts[n].store(x, std::memory_order_release); }
        Node *no_barrier_next(int n )            { assert(n >= 0); return nexts[n].load(std::memory_order_relaxed); }
        void no_barrier_set_next(int n, Node *x) { assert(n >= 0); nexts[n].store(x, std::memory_order_relaxed); }

        const Key key;
        std::atomic<Node*> nexts[1];     // array of next nodes with length equal to node height. next[0] is the lowest level link
    };

    // SkipList::Iterator
    // It doesn't implement stackdb::Iterator so as to avoid virtual function cost
    // and also doesn't provide key() & status() as specified by stackdb::Iterator
    template<typename Key, class Comparator>
    class SkipList<Key, Comparator>::Iterator {
    public:
        // init an iterator over the list. return iterator is not valid
        explicit Iterator(const SkipList *list): list(list), node(nullptr) {}
        // return true iff iterator is positioned at a valid node
        bool valid() const { return node != nullptr; }
        // return the ket at the current position. requires valid()
        const Key &key() const {  assert(valid()); return node->key; }
        // advance to the next position. requires valid()    
        void next() { assert(valid()); node = node->next(0); }
        // advance to the previous position. requires valid()
        void prev() {
            assert(valid());
            node = list->find_less_than(node->key);
            if (node == list->head) {
                node = nullptr;
            }
        }
        // advance to the first entry with a key >= target
        void seek(const Key &target) { 
            node = list->find_greater_or_equal(target, nullptr);
        }
        // position at the first entry. valid iff list is not empty
        void seek_to_first() { node = list->head->next(0); }
        // position at the last entry. valid iff list is not empty
        void seek_to_last() {
            node = list->find_last();
            if (node == list->head) {
                node = nullptr;
            }
        }

    private:
        const SkipList *list;
        struct Node *node;
    };

} 
#endif
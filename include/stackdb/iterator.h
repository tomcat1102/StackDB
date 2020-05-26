#ifndef STACKDB_ITERATOR_H
#define STACKDB_ITERATOR_H

#include "stackdb/slice.h"
#include "stackdb/status.h"

// Iterator specifies a interface to access a sequence of key/value pairs from a Table or DB
namespace stackdb {
    class Iterator {
    public:
        Iterator();
        Iterator(const Iterator&) = delete;
        Iterator& operator=(const Iterator&) = delete;

        virtual ~Iterator();
        //iterfaces
        virtual bool valid() const = 0;                 // valid iff source not empty
        virtual void seek_to_first() = 0;               // valid iff source not empty
        virtual void seek_to_last() = 0;                // valid iff source not empty
        virtual void seek(const Slice &target) = 0;     // position at first key in the source that is at or past target
        virtual void next() = 0;
        virtual void prev() = 0;
        virtual Slice key() = 0;
        virtual Slice value() = 0;
        virtual Status status() const = 0;

        // user can register functin/arg1/arg2 triples that are invoked when iterator is destroyed
        using cleanup_func = void (*)(void *arg1, void *arg2);
        void register_cleanup(cleanup_func func, void *arg1, void *arg2);

    private:
        // registered cleanup functions are stored in a singly-linked list
        struct cleanup_node {
            bool is_empty() const { return func == nullptr; }
            void run() {
                assert(func != nullptr);
                func(arg1, arg2);
            }

            cleanup_func func;
            void *arg1;
            void *arg2;
            cleanup_node *next;
        };
        cleanup_node cleanup_head;
    };

    // anonymous namespace to hide EmptyIterator
    // defined for new_empty_iterator() & new _error_iterator()
    namespace {
        class EmptyIterator: public Iterator {
        public:
            EmptyIterator(const Status& s): stat(s) {}
            ~EmptyIterator() override = default;

            bool valid() const override { return false; }
            void seek_to_first() override {}
            void seek_to_last() override {}
            void seek(const Slice &target) override {}
            void next() override { assert(false); }
            void prev() override { assert(false); }
            Slice key() override {
                assert(false);
                return Slice();
            }
            Slice value() override {
                assert(false);
                return Slice();
            }
            Status status() const override { return stat; }

        private:
            Status stat;
        };
    }

    inline Iterator* new_empty_iterator() {
        return new EmptyIterator(Status::OK());
    }
    inline Iterator *new_error_iterator(const Status &s) {
        return new EmptyIterator(s);
    }
}
#endif
#ifndef STACKDB_ITERATOR_H
#define STACKDB_ITERATOR_H

#include "stackdb/slice.h"

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
        virtual void seek(const Slice &target) = 0;     // position at first key in the source that is at or past target
        virtual void next() = 0;
        virtual void prev() = 0;
        virtual Slice key() = 0;
        virtual Slice value() = 0;
        // TODO: add status
    };
}

#endif
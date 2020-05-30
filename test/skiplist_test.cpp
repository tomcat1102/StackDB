#include <iostream>
#include <set>
#include "db/skiplist.h"
#include "util/arena.h"
#include "util/random.h"
using namespace stackdb;

typedef uint64_t Key;

struct Comparator {
    int operator()(const Key& a, const Key& b) const {
        if (a < b) {
            return -1;
        } else if (a > b) {
            return +1;
        } else {
            return 0;
        }
    }
};

int main() {
    // test empty
    {   
        Arena arena;
        Comparator cmp;
        SkipList<Key, Comparator> list(cmp, &arena);
        assert(!list.contains(10));

        SkipList<Key, Comparator>::Iterator iter(&list);
        assert(!iter.valid());
        iter.seek_to_first();
        assert(!iter.valid());
        iter.seek(100);
        assert(!iter.valid());
        iter.seek_to_last();
        assert(!iter.valid());
    }
    // test insert and lookup
    {
        const int N = 2000;
        const int R = 5000;
        Random rnd(1000);
        std::set<Key> keys;

        Arena arena;
        Comparator cmp;
        SkipList<Key, Comparator> list(cmp, &arena);

        for (int i = 0; i < N; i ++) {
            Key key = rnd.next() % R;
            if (keys.insert(key).second) {
                list.insert(key);
            }
        }

        for (int i = 0; i < R; i ++) {
            if (list.contains(i)) {
                assert(keys.count(i) == 1);
            } else {
                assert(keys.count(i) == 0);
            }
        }

        // simple iterator tests
        {
            SkipList<Key, Comparator>::Iterator iter(&list);
            assert(!iter.valid());

            iter.seek(0);
            assert(iter.valid());
            assert(*(keys.begin()) == iter.key());

            iter.seek_to_first();
            assert(iter.valid());
            assert(*(keys.begin()) == iter.key());

            iter.seek_to_last();
            assert(iter.valid());
            assert(*(keys.rbegin()) == iter.key());
        }

        // forward iteration test
        {
            for (int i = 0; i < R; i ++) {
                SkipList<Key, Comparator>::Iterator iter(&list);
                iter.seek(i);

                auto model_iter = keys.lower_bound(i);
                for (int j = 0; j < 3; j ++) {
                    if (model_iter == keys.end()) {
                        assert(!iter.valid());
                        break;
                    }

                    assert(iter.valid());
                    assert(iter.key() == *model_iter);

                    ++model_iter;
                    iter.next();
                }
            }
        }

        // backward iteration test
        {
            SkipList<Key, Comparator>::Iterator iter(&list);
            iter.seek_to_last();

            for (auto model_iter = keys.rbegin(); model_iter != keys.rend(); ++ model_iter) {
                assert(iter.valid());
                assert(iter.key() == *model_iter);
                iter.prev();
            }
            assert(!iter.valid());
        }
    }

    // NEED: add concurrent test for skiplist
    {
        // ...
    }
    return 0;
}
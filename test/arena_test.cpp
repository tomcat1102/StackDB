#include <iostream>
#include <vector>
#include <cassert>

#include "util/arena.h"
#include "util/random.h"
using namespace stackdb;

// Though I don't use google test framework, I try to mimic the test contents as in original leveldb
const int N = 100000;

int main () {
    std::vector<std::pair<size_t, char*>> allocated;
    Arena arena;
    size_t bytes = 0;
    Random rnd(301);

    for (int i = 0; i < N; i ++) {
        size_t s;
        if (i % (N / 10) == 0) {
            s = i;
        } else {
            s = rnd.one_in(4000) ? rnd.uniform(6000)
                    : (rnd.one_in(10) ? rnd.uniform(100) : rnd.uniform(20));
        }
        // Our arena disallows size 0 allocations.
        if (s == 0) { 
        s = 1;
        }

        char* r;
        if (rnd.one_in(10)) {
        r = arena.allocate_aligned(s);
        } else {
        r = arena.allocate(s);
        }

        for (size_t b = 0; b < s; b++) {
        // Fill the "i"th allocation with a known bit pattern
        r[b] = i % 256;
        }
        bytes += s;
        allocated.push_back(std::make_pair(s, r));
        assert(arena.get_mem_usage() >= bytes);
        if (i > N / 10) {
        assert(arena.get_mem_usage() <= bytes * 1.10);
        }
    }
    for (size_t i = 0; i < allocated.size(); i++) {
        size_t num_bytes = allocated[i].first;
        const char* p = allocated[i].second;
        for (size_t b = 0; b < num_bytes; b++) {
            // Check the "i"th allocation for the known bit pattern
            assert((int(p[b]) & 0xff) == i % 256);
        }
    }
    std::cout << "ok!" << std::endl;
}
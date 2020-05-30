#ifndef STACKDB_RANDOM_H
#define STACKDB_RANDOM_H

#include <cstdint>
#include <ctime>
#include <iostream>

namespace stackdb {
    // a simple random number generator
    class Random {
    public:
        explicit Random(uint32_t s): seed(s & 0x7fffffffu) {
            // Avoid bad seeds.
            if (seed == 0 || seed == 2147483647L) {
                seed = 1;
            }
        }
        static uint32_t time_seed() {
            return time(nullptr);
        }

        // return a pseudo random number in [0, 2^31 -1]
        uint32_t next() {
            uint64_t product = seed * A;
            seed = (product >> 31) + (product & M);
            if (seed > M) {
                seed -= M;
            }
            return seed;
        }

        // return a pseudo uniform number in [0, n - 1]
        uint32_t uniform(int n) { return next() % n; }
        // return true in 1/n of all time, false otherwise
        bool one_in(int n) { return (next() % n) == 0; }
        // I don't know this distribution
        uint32_t skewed(int max_log) { return uniform(1 << uniform(max_log + 1)); }

    private:
        const static uint32_t M = 2147483647L;
        const static uint64_t A = 16807;
        uint32_t seed;
    };

}


#endif
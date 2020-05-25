#ifndef STACKDB_SLICE_H
#define STACKDB_SLICE_H

#include <string>
#include <cstring>
#include <cassert>

namespace stackdb {
    class Slice {
    public:
        Slice(): sdata(""), ssize(0) {}
        Slice(const char *d): sdata(d), ssize(strlen(d)) {}
        Slice(const char *d, size_t sz): sdata(d), ssize(sz) {}
        Slice(const std::string& s): sdata(s.data()), ssize(s.size()) {}
        Slice(const Slice&) = default;
        // operators
        Slice &operator=(const Slice &) = default;
        char operator[](size_t n) const {
            assert(n < size());
            return sdata[n];
        }
        // others
        const char *data() const { return sdata; }
        size_t size() const { return ssize; }
        bool empty() const { return size() == 0; }
        void clear() {
            sdata = "";
            ssize = 0;
        }

        void remove_prefix(size_t n) {
            assert(n < size());
            sdata += n;
            ssize -= n;
        }
        bool starts_with(const Slice& s) const {
            return ssize >= s.ssize && memcmp(sdata, s.sdata, s.ssize) == 0;
        }

        //  we need 3-way comparision result so bool result from operator< is not enough
        int compare(const Slice& b) const {
            size_t min_len = (ssize < b.ssize ? ssize : b.ssize);
            int r = memcmp(sdata, b.sdata, min_len);
            if (r == 0) {
                if (ssize < b.ssize) {
                    return -1;
                } else if (ssize > b.ssize) {
                    return +1;
                }
            } 
            return r;
        }

        std::string to_string() const {
            return std::string(sdata, ssize);
        }

    private:
        const char *sdata;  // slice data
        size_t ssize;       // slice size
    };
}

#endif
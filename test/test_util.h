#ifndef STACKDB_TEST_UTIL_H
#define STACKDB_TEST_UTIL_H

#include "stackdb/slice.h"
#include "util/random.h"

namespace stackdb {
namespace test {
    Slice random_string(Random &rnd, int len, std::string &dst);
}
}

#endif
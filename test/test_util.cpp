#include "test_util.h"

namespace stackdb {
namespace test {

    Slice random_string(Random &rnd, int len, std::string &dst) {
        dst.resize(len);
        for (int i = 0; i < len; i ++) {
            dst[i] = ' ' + rnd.uniform(95);
        }
        return Slice(dst);
    }

}
}
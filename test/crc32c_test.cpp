#include <cstring>
#include <cassert>
#include "util/crc32c.h"
using namespace stackdb::crc32c;

int main() {
    // test standard results
    {
        // from rfc3720 section B.4.
        char buf[32];

        memset(buf, 0, sizeof(buf));
        assert(value(buf, sizeof(buf)) == 0x8a9136aa);

        memset(buf, 0xff, sizeof(buf));
        assert(value(buf, sizeof(buf)) == 0x62a8ab43);

        for (int i = 0; i < 32; i++) {
            buf[i] = i;
        }
        assert(value(buf, sizeof(buf)) == 0x46dd794e);

        for (int i = 0; i < 32; i++) {
            buf[i] = 31 - i;
        }
        assert(value(buf, sizeof(buf)) == 0x113fdb5c);

        uint8_t data[48] = {
            0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00,
            0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x18, 0x28, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        };
        assert(value(reinterpret_cast<char*>(data), sizeof(data)) == 0xd9963a56);
    }
    // test values not equal
    {
        assert(value("a", 1) != value("foo", 3));
    }
    // test extend
    {
        assert(value("hello world", 11) == extend(value("hello ", 6), "world", 5));
    }
    // test mask
    {
        uint32_t crc = value("foo", 3);
        assert(mask(crc) != crc);
        assert(mask(mask(crc)) != crc);
        assert(unmask(mask(crc)) == crc);
        assert(unmask(unmask(mask(mask(crc)))) == crc);
    }
}

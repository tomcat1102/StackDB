#include <cstring>
#include <cassert>

#include "stackdb/status.h"
using namespace stackdb;

Status::Status(Code code, const Slice &msg, const Slice &msg2) {
    assert(code != Code::Ok);
    const uint32_t len1 = msg.size();
    const uint32_t len2 = msg2.size();
    const uint32_t size = len1 + (len2 ? (2 + len2) : 0);
    char *result = new char[size + 5];

    std::memcpy(result, &size, sizeof(size));
    result[4] = static_cast<char>(code);
    std::memcpy(result + 5, msg.data(), len1);
    if (len2) {
        result[5 + len1] = ':';
        result[6 + len1] = ' ';
        std::memcpy(result + 7 + len1, msg2.data(), len2);
    }
    // must assign at last, since state is const char*
    state = result;
}

const char *Status::copy_state(const char *state) {
    uint32_t size;
    std::memcpy(&size, state, sizeof(size));
    char *result = new char[size + 5];
    std::memcpy(result, state, size + 5);
    return result;
}

std::string Status::to_string() const{
    uint32_t length;
    memcpy(&length, state, sizeof(length));

    const char *type;
    switch (code()) {
        case Code::Ok:              type = "OK"; break;
        case Code::NotFound:        type = "NotFound: "; break;
        case Code::Corruption:      type = "Corruption: "; break;
        case Code::NotSupported:    type = "Not implemented: "; break;
        case Code::InvalidArgument: type = "Invalid argument: "; break;
        case Code::IOError:         type = "IO error: "; break;
        default:                    type = "Unknown code"; break;
    }

    return std::string(type).append(state + 5, length);
}

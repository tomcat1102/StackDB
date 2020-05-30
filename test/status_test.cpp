#include <iostream>
#include <cassert>
#include "stackdb/status.h"
using namespace stackdb;

int main() {
    // test move constructor
    {
        Status ok = Status::OK();
        Status ok2 = std::move(ok);
        assert(ok2.ok() == true);
    }
    {
        Status status = Status::NotFound("custom NotFound status message");
        Status status2 = std::move(status);
        assert(status2.is_not_found() == true);
        assert(status2.to_string().compare("NotFound: custom NotFound status message") == 0);
    }
    {
        Status self_moved = Status::IOError("custom IOError status message");
        Status& self_moved_reference = self_moved;
        self_moved_reference = std::move(self_moved);
    }
    return 0;
}
#ifndef STACKDB_LOGGING_H
#define STACKDB_LOGGIN_H

#include <string>
#include "stackdb/slice.h"

namespace stackdb {
    // append a readable printout of num to *str
    void append_number_to(std::string *str, uint64_t num);
    // append value to *str with non-printable chracters escaped 
    void append_escaped_string_to(std::string *str, const Slice &value);
    // return a readable string from num
    std::string number_to_string(uint64_t num);
    // return a readable string from value with non-printable characters escaped
    std::string escape_string(const Slice &value);
    // try parse a number from *in. if ok, advance in, return number via val and
    // return true. otherwise leave in untouched and return false
    bool consume_decimal_number(Slice *in, uint64_t *val);
}

#endif
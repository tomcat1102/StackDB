#include <iostream>
#include <limits>
#include <cassert>

#include "util/logging.h"
using namespace stackdb;

static void consume_decimal_number_test(uint64_t number,  const std::string& padding = "") {
    std::string decimal_number = number_to_string(number);
    std::string input_string = decimal_number + padding;

    Slice input(input_string);
    Slice output = input;

    uint64_t result;

    assert(consume_decimal_number(&output, &result));
    assert(number == result);
    assert(decimal_number.size() == static_cast<size_t>(output.data() - input.data()));
    assert(padding.size() ==output.size());
}

static void consume_decimal_number_overflow_test(const std::string& input_string) {
    Slice output(input_string);
    uint64_t result;
    assert(!consume_decimal_number(&output, &result));
}

static void consume_decimal_number_no_digits_test(const std::string& input_string) {
    Slice input(input_string);
    Slice output = input;
    uint64_t result;

    assert(!consume_decimal_number(&output, &result));
    assert(input.data() == output.data());
    assert(input.size() == output.size());
}

int main () {
    // test number to string
    {
        assert(number_to_string(0) == "0");
        assert(number_to_string(1) == "1");
        assert(number_to_string(9) == "9");

        assert(number_to_string(10) == "10");
        assert(number_to_string(11) == "11");
        assert(number_to_string(19) == "19");
        assert(number_to_string(99) == "99");

        assert(number_to_string(100) == "100");
        assert(number_to_string(109) == "109");
        assert(number_to_string(190) == "190");
        assert(number_to_string(123) == "123");
        assert(number_to_string(12345678) == "12345678");

        static_assert(std::numeric_limits<uint64_t>::max() == 18446744073709551615U,
            "Test consistency check");
        assert(number_to_string(18446744073709551000U) == "18446744073709551000");
        assert(number_to_string(18446744073709551610U) == "18446744073709551610");
        assert(number_to_string(18446744073709551614U) == "18446744073709551614");
        assert(number_to_string(18446744073709551615U) == "18446744073709551615");
        assert(number_to_string(18446744073709551600U) == "18446744073709551600");
    }
    // test consume decimal numbers
    {
        consume_decimal_number_test(0);
        consume_decimal_number_test(1);
        consume_decimal_number_test(9);

        consume_decimal_number_test(10);
        consume_decimal_number_test(11);
        consume_decimal_number_test(19);
        consume_decimal_number_test(99);

        consume_decimal_number_test(100);
        consume_decimal_number_test(109);
        consume_decimal_number_test(190);
        consume_decimal_number_test(123);

        for (uint64_t i = 0; i < 100; ++i) {
            uint64_t large_number = std::numeric_limits<uint64_t>::max() - i;
            consume_decimal_number_test(large_number);
        }
    }
    // test consume decimal number with paddings
    {
        consume_decimal_number_test(0, " ");
        consume_decimal_number_test(1, "abc");
        consume_decimal_number_test(9, "x");

        consume_decimal_number_test(10, "_");
        consume_decimal_number_test(11, std::string("\0\0\0", 3));
        consume_decimal_number_test(19, "abc");
        consume_decimal_number_test(99, "padding");

        consume_decimal_number_test(100, " ");

        for (uint64_t i = 0; i < 100; ++i) {
            uint64_t large_number = std::numeric_limits<uint64_t>::max() - i;
            consume_decimal_number_test(large_number, "pad");
        }
    }
    // test consume decimal number overflow
    {
        static_assert(std::numeric_limits<uint64_t>::max() == 18446744073709551615U, "Test consistency check");
        consume_decimal_number_overflow_test("18446744073709551616");
        consume_decimal_number_overflow_test("18446744073709551617");
        consume_decimal_number_overflow_test("18446744073709551618");
        consume_decimal_number_overflow_test("18446744073709551619");
        consume_decimal_number_overflow_test("18446744073709551620");
        consume_decimal_number_overflow_test("18446744073709551621");
        consume_decimal_number_overflow_test("18446744073709551622");
        consume_decimal_number_overflow_test("18446744073709551623");
        consume_decimal_number_overflow_test("18446744073709551624");
        consume_decimal_number_overflow_test("18446744073709551625");
        consume_decimal_number_overflow_test("18446744073709551626");
        consume_decimal_number_overflow_test("18446744073709551700");
        consume_decimal_number_overflow_test("99999999999999999999");
    }
    // test consume decimal number without digits
    {
        consume_decimal_number_no_digits_test("");
        consume_decimal_number_no_digits_test(" ");
        consume_decimal_number_no_digits_test("a");
        consume_decimal_number_no_digits_test(" 123");
        consume_decimal_number_no_digits_test("a123");
        consume_decimal_number_no_digits_test(std::string("\000123", 4));
        consume_decimal_number_no_digits_test(std::string("\177123", 4));
        consume_decimal_number_no_digits_test(std::string("\377123", 4));
    }
    return 0;
}

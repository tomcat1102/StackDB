#ifndef STACKDB_STATUS_H
#define STACKDB_STATUS_H

#include <utility>          // for std::swap
#include "stackdb/slice.h"


// Ok status has a null state, other status have the form:
//    state_[0..3] == length of message
//    state_[4]    == code
//    state_[5..]  == message
namespace stackdb {
    class Status {
        // possible status codes. scoped to avoid confict with static function names
        enum class Code{
            Ok = 0,
            NotFound = 1,
            Corruption = 2,
            NotSupported = 3,
            InvalidArgument = 4,
            IOError = 5
        };

    public:
        // constructors and operators
        Status() noexcept: state(nullptr) {}
        Status(const Status& rhs) { state = (rhs.state == nullptr) ? nullptr : copy_state(rhs.state); }
        Status(Status &&rhs) noexcept: state(rhs.state) { rhs.state = nullptr; }
        ~Status() { delete[] state; }

        Status& operator=(const Status& rhs) {
            if (state != rhs.state) {
                delete[] state;
                state = (rhs.state == nullptr) ? nullptr : copy_state(rhs.state);
            }
            return *this;
        }
        Status& operator=(Status&& rhs) noexcept {
            std::swap(state, rhs.state);
            return *this;
        }

        // static function to construct each status. Node not to mix up OK(), ok() and enum Ok
        static Status OK() { 
            return Status();
        }
        static Status NotFound(const Slice& msg, const Slice& msg2 = Slice()) { 
            return Status(Code::NotFound, msg, msg2);
        }
        static Status Corruption(const Slice& msg, const Slice& msg2 = Slice()) {
            return Status(Code::Corruption, msg, msg2);
        }
        static Status NotSupported(const Slice& msg, const Slice& msg2 = Slice()) {
            return Status(Code::NotSupported, msg, msg2);
        }
        static Status InvalidArgument(const Slice& msg, const Slice& msg2 = Slice()) {
            return Status(Code::InvalidArgument, msg, msg2);
        }
        static Status IOError(const Slice& msg, const Slice& msg2 = Slice()) {
            return Status(Code::IOError, msg, msg2);
        }

        // check each status
        bool ok() const { return state == nullptr; }
        bool is_not_found() const { return code() == Code::NotFound; }
        bool is_corruption() const { return code() == Code::Corruption; }
        bool is_not_supported_error() const { return code() == Code::NotSupported; }
        bool is_invalid_argument() const { return code() == Code::InvalidArgument; }
        bool is_io_error() const { return code() == Code::IOError; }

        std::string to_string() const;

    private:
        // private helper constructor
        Status(Code code, const Slice &msg, const Slice &msg2);
        // get status code in *state
        Code code() const { return (state == nullptr) ? Code::Ok : static_cast<Code>(state[4]); }
        static const char *copy_state(const char *state);

    private:
        const char *state;
    };
}

#endif
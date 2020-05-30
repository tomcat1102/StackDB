#ifndef STACKDB_ENV_POSIX_TEST_HELPER_H
#define STACKDB_ENV_POSIX_TEST_HELPER_H

// helper functions to set fd and mmap limit in PosixEnv, respectively
namespace stackdb {
    void set_read_fd_limit(int limit);
    void set_mmap_limit(int limit);
} // namespace stackdb
#endif
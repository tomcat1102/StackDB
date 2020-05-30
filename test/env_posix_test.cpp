#include <unistd.h>
#include <sys/resource.h>
#include <sys/wait.h>

#include <iostream>
#include <string>
#include <limits>
#include <unordered_set>
#include <cstdlib>
#include "stackdb/env.h"
#include "env_posix_test_helper.h"
using namespace stackdb;


// helper config and function to set the two limits
static const int READ_FD_LIMIT = 4;
static const int MMAP_LIMIT = 4;    
static void set_file_limits(int read_fd_limit, int mmap_limit) {
    set_read_fd_limit(read_fd_limit);
    set_mmap_limit(mmap_limit);
}
#if HAVE_O_CLOEXEC
// helper exict codes for debugging Close On Exec. 61, 62, 63 have no special meaning
static const int COE_ERROR_CHILD = 61;
static const int COE_ERROR_DUP2 = 62;
static const int COE_ERROR_FOUND_OPEN = 63;
// hidden option to differ main test process from child coe process
static const char COE_OPTION[] = "--test-close-on-exec";

// helper functions for tests in main
// child exec on fork(). since fd open with O_CLOEXEC, fd in child shouldn't be open
static int child_coe_main(char *fd_arg) {
    int fd = atoi(fd_arg);
    // check fd 's not open
    if (dup2(fd, fd) == fd) { 
        fprintf(stderr, "unexpectd open fd %d\n", fd);
        return COE_ERROR_FOUND_OPEN;
    }
    // check above dup2() failed due to open, not the other around
    if (errno != EBADF) {
        fprintf(stderr, "unexpected errno after calling dup2 on fd %d: %s",
            fd, strerror(errno));
        return COE_ERROR_DUP2;
    }
    return 0;
}
// max possible fd in the system
static int get_max_fd() {
    struct rlimit limit;
    assert(getrlimit(RLIMIT_NOFILE, &limit) == 0);
    return limit.rlim_cur;
}
// iterate and record each possible open fd
static std::unordered_set<int> get_open_fds() {
    int max_fd = get_max_fd();
    std::unordered_set<int> open_fds;
    // if fd is open, dup2 reopens it at fd again and returns fd.
    // if not open, dup2 returns -1 and sets errno
    for (int fd = 0; fd < max_fd; fd ++) {
        if (dup2(fd, fd) == fd) { // ok
            open_fds.insert(fd);
        } else {
            assert(errno == EBADF);
        }
    }
    return open_fds;
}
// find current fd that is newly opened sinece previous get_open_fds()
static int get_new_open_fd(std::unordered_set<int> base_fds) {
    std::unordered_set<int> open_fds = get_open_fds();
    for (int fd: base_fds) {
        assert(open_fds.count(fd) == 1); // previous fd shouldn't have been closed
        open_fds.erase(fd);
    }
    assert(open_fds.size() == 1);    // only one new fd
    return *open_fds.begin();
}
// check fork() + exec() child has no extra fd
static void check_coe_no_leak_fds(std::unordered_set<int> &base_fds) {
    char arg0[] = "env_posix_test";
    char arg1[sizeof(COE_OPTION)];
    memcpy(arg1, COE_OPTION, sizeof(COE_OPTION));
    char arg2[32] = {0};
    snprintf(arg2, sizeof(arg2), "%d", get_new_open_fd(base_fds));

    // child exec main() and further child_coe_main()
    char *child_argv[] = {arg0, arg1, arg2, nullptr};
    int child_pid = fork();
    if (child_pid == 0) { 
        execv(child_argv[0], child_argv);
        fprintf(stderr, "error spawning child process %s\n", strerror(errno));
        fflush(stderr);
        exit(COE_ERROR_CHILD);
    }
    // father wait and check
    int child_status = 0;
    assert(waitpid(child_pid, &child_status, 0) == child_pid);
    assert(WIFEXITED(child_status));
    assert(WEXITSTATUS(child_status) == 0);
}
#endif // HAVE_O_CLOEXEC

// on main, four fds already open for cin, cout, cerr and possibly clogs
int main(int argc, char *argv[]) {
    // child:
    // check whether contains coe option, if coe supported
#if HAVE_O_CLOEXEC
    for (int i = 0; i < argc; i ++) {
        if (strcmp(argv[i], COE_OPTION) == 0) {
            int res =  child_coe_main(argv[i + 1]);
            return res;
        }
    }
#endif

    // main:
    // set proper limits to avoid them being too large to test in posix os like linux, before Env::get_default()
    set_file_limits(READ_FD_LIMIT, MMAP_LIMIT);
    Env *env = Env::get_default();
    // test open on read
    {
        std::string test_dir;
        assert(env->get_test_dir(&test_dir).ok());
        std::string test_file = test_dir + "/open_on_read.txt";

        FILE *fp = fopen(test_file.c_str(), "w");
        assert(fp != nullptr);
        const char file_data[] = "abcdefghijklmnopqrstuvwxyz";
        fputs(file_data, fp);
        fclose(fp);

        // open test file some number above the sum of two limits to
        // force open-on-read behavior of PosixEnv::RandomAccessFile
        int num_files = READ_FD_LIMIT + MMAP_LIMIT + 5;
        RandomAccessFile* files[num_files] = {0};
        for (int i = 0; i < num_files; i++) {
            assert(env->new_random_access_file(test_file, &files[i]).ok());
        }

        char scratch;
        Slice read_result;
        for (int i = 0; i < num_files; i++) {
            assert(files[i]->read(i, 1, &read_result, &scratch).ok());
            assert(file_data[i] == read_result[0]);
        }
        for (int i = 0; i < num_files; i++) {
            delete files[i];
        }
        assert(env->remove_file(test_file).ok());
    }

#if HAVE_O_CLOEXEC
    // test close on sequential file
    {
        std::unordered_set<int> open_fds = get_open_fds();
        std::string test_dir;
        assert(env->get_test_dir(&test_dir).ok());
        std::string file_path = test_dir + "/close_on_exec_seq_file.txt";
        assert(write_string_to_file(env, "0123456789", file_path).ok());

        SequentialFile *file = nullptr;
        assert(env->new_sequential_file(file_path, &file).ok());
        // test close on exec
        check_coe_no_leak_fds(open_fds);
        // cleanup after test
        delete file;
        assert(env->remove_file(file_path).ok());
    }
    // test close on random access file
    {
        std::unordered_set<int> open_fds = get_open_fds();

        std::string test_dir;
        assert(env->get_test_dir(&test_dir).ok());
        std::string file_path = test_dir + "/close_on_exec_random_access.txt";
        assert(write_string_to_file(env, "0123456789", file_path).ok());
        // exhast mmap limit to stest file backed by fd not mmap region
        RandomAccessFile *mmaped_files[MMAP_LIMIT] = {nullptr};
        for (int i = 0; i < MMAP_LIMIT; i ++) {
            assert(env->new_random_access_file(file_path, &mmaped_files[i]).ok());
        }
        RandomAccessFile *file = nullptr;
        assert(env->new_random_access_file(file_path, &file).ok());
        std::unordered_set<int> open_fds2 = get_open_fds();
        // test close on exec
        check_coe_no_leak_fds(open_fds);
        // cleanup after test
        delete file;
        for (int i = 0; i < MMAP_LIMIT; i++)  
            delete mmaped_files[i];
        assert(env->remove_file(file_path).ok());
    }
    // test close on writable file
    {
        std::unordered_set<int> open_fds = get_open_fds();
        std::string test_dir;
        assert(env->get_test_dir(&test_dir).ok());
        std::string file_path = test_dir + "/close_on_exec_writable.txt";
        assert(write_string_to_file(env, "0123456789", file_path).ok());

        WritableFile *file = nullptr;
        assert(env->new_writable_file(file_path, &file).ok());
        // test close on exec
        check_coe_no_leak_fds(open_fds);
        // cleanup after test
        delete file;
        assert(env->remove_file(file_path).ok());
    }
    // test close on appendable file
    {
        std::unordered_set<int> open_fds = get_open_fds();
        std::string test_dir;
        assert(env->get_test_dir(&test_dir).ok());
        std::string file_path = test_dir + "/close_on_exec_appendable.txt";
        assert(write_string_to_file(env, "0123456789", file_path).ok());

        WritableFile *file = nullptr;
        assert(env->new_appendable_file(file_path, &file).ok());
        // test close on exec
        check_coe_no_leak_fds(open_fds);
        // cleanup after test
        delete file;
        assert(env->remove_file(file_path).ok());
    }   
    // test close on lock file
    {
        std::unordered_set<int> open_fds = get_open_fds();
        std::string test_dir;
        assert(env->get_test_dir(&test_dir).ok());
        std::string file_path = test_dir + "/close_on_exec_lock.txt";
        assert(write_string_to_file(env, "0123456789", file_path).ok());

        FileLock *lock = nullptr;
        assert(env->lock_file(file_path, &lock).ok());
        // test close on exec
        check_coe_no_leak_fds(open_fds);
        assert(env->unlock_file(lock).ok());
        // cleanup after test
        assert(env->remove_file(file_path).ok());
    }
    // test close on logger
    {
        std::unordered_set<int> open_fds = get_open_fds();
        std::string test_dir;
        assert(env->get_test_dir(&test_dir).ok());
        std::string file_path = test_dir + "/close_on_exec_logger.txt";
        assert(write_string_to_file(env, "0123456789", file_path).ok());

        Logger *logger = nullptr;
        assert(env->new_logger(file_path, &logger).ok());
        // test close on exec
        check_coe_no_leak_fds(open_fds);
        // cleanup after test
        delete logger;
        assert(env->remove_file(file_path).ok());
    }
#endif  // HAVE_O_CLOEXEC  
    return 0;
}

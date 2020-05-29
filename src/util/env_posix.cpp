// system headers
#include <unistd.h>         // close(), leek(), fdatasync()/fsync()
#include <fcntl.h>          // HAVE_O_CLOEXEC
#include <dirent.h>         // DIR, opendir()
#include <sys/mman.h>       // mmap(), munmap()
#include <sys/resource.h>   // rlimit, getrlimit()
#include <sys/stat.h>       // stat()
#include <sys/time.h>       // gettimeofday()

#include <iostream>
#include <sstream>
#include <atomic>
#include <set>
#include <limits>
#include <thread>
#include <cstdio>           // rename(), fwrite(), fflush(), fclose()...
#include <cstdlib>          // getenv()
#include <cstdarg>
#include <cerrno>           // ENOENT, EINTR, EINVAL
#include <cstring>          // strerror()
#include "stackdb/env.h"

// NEED: LockTable requires lock primitives. PosixEnv requires many more
namespace stackdb {
    int config_read_fd_limit = -1;                                          // limit on number of open read-only fds. if < 0, reset by max_open_files()
    const static int DEFAULT_MMAP_LIMIT = (sizeof(void *) >= 8) ? 1000 : 0; // up to 1000 mmap regions for 64-bit binaries, none for 32-bit, 
    const static int WRITABLE_FILE_BUFFER_SIZE = 64 * 1024;                 //  because 64-bit has much more virtual mem space for mmap()
#if HAVE_O_CLOEXEC
    const static int OPEN_BASE_FLAGS = O_CLOEXEC;
#else
    const static int OPEN_BASE_FLAGS = 0;
#endif

    // represent posix error with Status
    static Status posix_error(const std::string &context, int err_num) {
        if (err_num == ENOENT) {
            return Status::NotFound(context, std::strerror(err_num));
        } else {
            return Status::IOError(context, std::strerror(err_num));
        }
    }

    // helper class to limit resource usage to avoid exhaustion and hence error
    // used for read-only and mmap files
    class Limiter {
    public:     
        Limiter(int max_acquires): acquires_allowed(max_acquires) {}
        // true if there are available resources, false otherwise
        bool acquire() {
            int old_acquires = acquires_allowed.fetch_sub(1, std::memory_order_relaxed);
            if (old_acquires > 0) return true;
            acquires_allowed.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
        // release resource obtained from acquire()
        void release() { acquires_allowed.fetch_add(1, std::memory_order_relaxed); }
    private:
        std::atomic<int> acquires_allowed; // num of available resources
    };

    // posix implementaion for SequentialFile
    class PosixSequentialFile final : public SequentialFile {
    public:
        PosixSequentialFile(std::string filename, int fd)
            : fd(fd), filename(filename) {}
        ~PosixSequentialFile() override { ::close(fd); }
        // interfaces
        Status read(size_t n, Slice *result, char *scratch) override {
            while (true) {  // read once unless interrupted
                ssize_t nread = ::read(fd, scratch, n);
                if (nread < 0) {
                    if (errno == EINTR) continue;
                    return posix_error(filename, errno);
                }
                *result = Slice(scratch, nread);
                break;
            }
            return Status::OK();
        }
        Status skip(uint64_t n) override {
            if (::lseek(fd, n, SEEK_CUR) == -1) { // off_t
                return posix_error(filename, errno);
            }
            return Status::OK();
        }
    private:
        const int fd;
        const std::string filename;
    };

    // posix implementaion for RandomAccessFile, using pread()
    class PosixRandomAccessFile final : public RandomAccessFile {
    public:
        // takes ownership of fd. fd_limiter shall outlive this due to release() in destructor
        PosixRandomAccessFile(std::string filename, int fd, Limiter* fd_limiter)
            : has_permanent_fd(fd_limiter->acquire()),
              fd(has_permanent_fd ? fd : -1),
              fd_limiter(fd_limiter),
              filename(filename) {
            if (!has_permanent_fd) {
                assert(fd != -1);
                ::close(fd);    
            }
        }
        // release this fd resource, if fd is permanent
        ~PosixRandomAccessFile() override {
            if (has_permanent_fd) {
                assert(fd != -1);
                ::close(fd);
                fd_limiter->release();
            }
        }
        // interfaces
        Status read(uint64_t offset, size_t n, Slice* result, char* scratch) const override {
            int fd_ = fd;
            // open if no permanent fd
            if (!has_permanent_fd) {
                fd_ = ::open(filename.c_str(), O_RDONLY | OPEN_BASE_FLAGS);
                if (fd_ < 0)
                    return posix_error(filename, errno);
            }
            assert(fd_ != -1);
            // read from fd_
            Status status;
            ssize_t nread = ::pread(fd_, scratch, n, offset);
            *result = Slice(scratch, (nread < 0) ? 0 : nread);
            if (nread < 0) {
                status = posix_error(filename, errno);
            }
            // close fd if it's not permanent
            if (!has_permanent_fd) {
                assert(fd_ != fd);
                ::close(fd_);
            }
            return status;
        }
    private:
        const bool has_permanent_fd;    // fixed fd for each random read. if false, open file on each read
        const int fd;                   // -1 if has_permanent_fd is false
        Limiter *const fd_limiter;
        const std::string filename;
    };

    // posix implementaion for RandomAccessFile, using mmap()
    class PosixMmapReadableFile final : public RandomAccessFile {
    public:
        // mmap_base[0, len - 1] refer to memory-mapped contents of whole file from a successful call to mmap()
        // takes owenership of the region. fd_limiter shall outlive this due to release() in destructor
        PosixMmapReadableFile(std::string filename, char *mmap_base, size_t len, Limiter *mmap_limiter)
            : mmap_base(mmap_base), len(len), mmap_limiter(mmap_limiter), filename(filename) {}
        ~PosixMmapReadableFile() override {
            ::munmap(mmap_base, len);
            mmap_limiter->release();
        }
        // interface
        Status read(uint64_t offset, size_t n, Slice* result, char* scratch) const override {
            if (offset + n > len) {
                *result = Slice();
                return posix_error(filename, EINVAL);
            }
            *result = Slice(mmap_base + offset, n);
            return Status::OK();
        }
    private:
        char *const mmap_base;
        const size_t len;
        Limiter *const mmap_limiter;
        const std::string filename;
    };

    // posix implementation for WritableFile
    class PosixWritableFile final : public WritableFile {
    public:
        PosixWritableFile(std::string filename, int fd)
            : pos(0), fd(fd), is_manifest(is_manifest_file(filename)),
              filename(filename), dirname(get_dirname(filename)) {}
        ~PosixWritableFile() override {
            if (fd >= 0) {
                close();
            }
        }
        // interfaces
        Status append(const Slice& data) override {
            size_t write_size = data.size();
            const char *write_data = data.data();

            // fit as much as possible into buffer
            size_t copy_size = std::min(write_size, WRITABLE_FILE_BUFFER_SIZE - pos);
            std::memcpy(buf + pos, write_data, copy_size);
            write_data += copy_size;
            write_size -= copy_size;
            pos += copy_size;
            if (write_size == 0) {
                return Status::OK();
            }

            // cannot fit in buffer, so need to do at least one write
            Status status = flush_buffer();
            if (!status.ok()) {
                return status;
            }
            // small writes go to buffer, large writes are written directly
            if (write_size < WRITABLE_FILE_BUFFER_SIZE) {
                std::memcpy(buf, write_data, write_size);
                pos = write_size;
                return Status::OK();
            }
            return write_unbuffered(write_data, write_size);
        }
        Status close() override {
            Status status = flush_buffer();
            int res = ::close(fd);
            if (res < 0 && status.ok()) {
                status = posix_error(filename, errno);
            }
            fd = -1;
            return status;
        }
        Status flush() override {
            return flush_buffer();
        }
        Status sync() override {
            // ensure new files referred by manifest are in the filesystem
            // before manifest is flushed to disk, avoiding inconsistency.
            Status status;
            if (is_manifest) {
                status = sync_dir();
                if (!status.ok())
                    return status;
            }

            // flush internal buf
            status = flush_buffer();
            if (!status.ok()) {
                return status;
            }
            // sync to system buf
            return sync_to_disk(fd, filename);
        }

    private:
        Status flush_buffer() {
            Status status = write_unbuffered(buf, pos);
            pos = 0;
            return status;
        }
        Status write_unbuffered(const char *data, size_t size) {
            while (size > 0) {
                ssize_t nwrite = ::write(fd, data, size);
                if (nwrite < 0) {
                    if (errno == EINTR) continue;
                    return posix_error(filename, errno);
                }
                data += nwrite;
                size -= nwrite;
            }
            return Status::OK();
        }
        Status sync_dir() {
            int fd = ::open(dirname.c_str(), O_RDONLY | OPEN_BASE_FLAGS);
            if (fd < 0) {
                return posix_error(dirname, errno);
            } 

            Status status = sync_to_disk(fd, dirname);
            ::close(fd);    // sync matters, ignore any close() error
            return status;
        }
        // sync fd's system buf to persistent disk. fd_path for Status description
        Status sync_to_disk(int fd, const std::string &fd_path) {
        #if HAVE_FULLFSYNC
            if (::fcntl(fd, F_FULLFSYNC) == 0) {
                return Status::OK();
            }
        #endif
        #if HAVE_FDATASYNC
            bool sync_ok = ::fdatasync(fd) == 0;
        #else
            bool sync_ok = ::fsync(fd) == 0;
        #endif
            if (sync_ok) 
                return Status::OK();
            return posix_error(fd_path, errno);
        }

        // extract dir name from a path, return '.' if no separator
        std::string get_dirname(const std::string &filename) {
            auto separator_pos = filename.rfind('/');
            if (separator_pos == std::string::npos) {
                return ".";
            }
            // check that really no separator after separator_pos
            assert(filename.find('/', separator_pos + 1) == std::string::npos);
            return filename.substr(0, separator_pos);
        }
        // extract file name from a path
        Slice basename(const std::string &filename) {
            auto separator_pos = filename.rfind('/');
            if (separator_pos == std::string::npos) {
                return Slice(filename);
            }
            // check that really no separator after separator_pos
            assert(filename.find('/', separator_pos + 1) == std::string::npos);
            return Slice(filename.data() + separator_pos + 1,
                         filename.size() - separator_pos - 1);
        }
        bool is_manifest_file(const std::string &filename) {
            return basename(filename).starts_with("MANIFEST");
        }

        // buf[0, pos - 1] contains data to be written to fd
        char buf[WRITABLE_FILE_BUFFER_SIZE];
        size_t pos;

        int fd;
        const bool is_manifest;     // true if filename starts with MANIFEST
        const std::string filename;
        const std::string dirname;
    };

    // posix implementaion for Logger
    class PosixLogger final : public Logger {
    public:
        // takes ownership of fp. write log to the file
        explicit PosixLogger(FILE *fp) : fp(fp) { assert(fp != nullptr); }
        ~PosixLogger() override { fclose(fp); }

        // interface. 
        void logv(const char *format, va_list args) override {
            // record time as close to lov() as possible
            struct timeval now_tv;
            gettimeofday(&now_tv, nullptr);
            time_t now_seconds = now_tv.tv_sec;
            struct tm now;
            localtime_r(&now_seconds, &now);  // convert epoch seconds to local time

            // record thread id. convert thread id type to string type of proper size
            const int MAX_THREAD_ID_SIZE = 32;
            std::ostringstream thread_stream;
            thread_stream << std::this_thread::get_id();
            std::string thread_id = thread_stream.str();
            if (thread_id.size() > MAX_THREAD_ID_SIZE) {
                thread_id.resize(MAX_THREAD_ID_SIZE);
            }

            // try stack buf in first iter, or dynamic buf if too large in later iter
            const int STACK_BUF_SIZE = 512;
            char stack_buf[STACK_BUF_SIZE];
            static_assert(sizeof(stack_buf) == STACK_BUF_SIZE);
            int dynamic_buf_size = 0;
            for (int iter = 0; iter < 2; iter ++) {
                int buf_size = (iter == 0) ? STACK_BUF_SIZE : dynamic_buf_size;
                char *buf = (iter == 0) ? stack_buf : new char[dynamic_buf_size];
                // print header into buf. 'year/month/day-hour:minute:second:micro threadid'
                int buf_offset = snprintf(buf, buf_size, "%04d/%02d/%02d-%02d:%02d:%02d.%06d %s\n",
                    now.tm_year + 1900, now.tm_mon + 1, now.tm_mday, 
                    now.tm_hour, now.tm_min, now.tm_sec, static_cast<int>(now_tv.tv_usec),
                    thread_id.c_str());

                assert(buf_offset < 28 + MAX_THREAD_ID_SIZE);   // 28 = 10 date + 15 time + 3 delimiter
                assert(buf_offset < buf_size);

                // print formated args
                va_list args_copy;          // copy if case failed due to no space
                va_copy(args_copy, args);
                buf_offset += vsnprintf(buf + buf_offset, buf_size - buf_offset, format, args_copy);
                va_end(args_copy);
                
                if (buf_offset >= buf_size) {
                    if (iter == 0) dynamic_buf_size = buf_offset;
                    assert(false);  // can't fail on dynamic iter
                }

                assert(buf_offset <= buf_size);
                fwrite(buf, 1, buf_offset, fp); // actual writing
                fflush(fp);

                if (iter != 0) delete[] buf;
            }
        }
    private:
        FILE *const fp;
    };

    // posix implementation for FileLock.
    class PosixFileLock final : public FileLock {
    public:
        PosixFileLock(int fd, std::string filename)
            : fd(fd), filename(filename) {}

        int get_fd() const { return fd; }
        const std::string &get_filename() const { return filename; }
    private:
        const int fd;
        const std::string filename;
    };

    // posix environment
    class PosixEnv : public Env {
    public:        
        PosixEnv(): mmap_limiter(max_mmaps()), fd_limiter(max_open_fds()) {}
        ~PosixEnv() override {
            static const char msg[] =
                "PosixEnv singletion destroyed. Unsupported behavior!";
            std::fwrite(msg, 1, sizeof(msg), stderr);
            std::abort();
        }

        // final interfaces
        Status new_sequential_file(const std::string& fname, SequentialFile** result) override {
            int fd = ::open(fname.c_str(), O_RDONLY | OPEN_BASE_FLAGS);
            if (fd < 0) {
                *result = nullptr;
                return posix_error(fname, errno);
            }
            *result = new PosixSequentialFile(fname, fd);
            return Status::OK();
        }

        Status new_random_access_File(const std::string& fname, RandomAccessFile** result) override {
            int fd = ::open(fname.c_str(), O_RDONLY | OPEN_BASE_FLAGS);
            if (fd < 0) {
                *result = nullptr;
                return posix_error(fname, errno);
            }
            if (!mmap_limiter.acquire()) { // if no mmap() available, resort to normal random access file
                *result = new PosixRandomAccessFile(fname, fd, &fd_limiter);
            }

            uint64_t file_size; // prepare file size to map whole file
            Status status = get_file_size(fname, &file_size);
            if (status.ok()) {
                void *mmap_base = mmap(/*addr=*/nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
                if (mmap_base != MAP_FAILED) {
                    *result = new PosixMmapReadableFile(fname, (char*)mmap_base, file_size, &mmap_limiter);
                } else {
                    status = posix_error(fname, errno);
                }
            }
            close(fd);
            if (!status.ok()) {
                mmap_limiter.release();
            }
            return status;
        }

        Status new_writable_file(const std::string& fname, WritableFile** result) override {
            int fd = open(fname.c_str(), O_TRUNC | O_WRONLY | O_CREAT | OPEN_BASE_FLAGS, 0644);
            if (fd < 0) {
                *result = nullptr;
                return posix_error(fname, errno);
            }
            *result = new PosixWritableFile(fname, fd);
            return Status::OK();
        }

        Status new_appendable_file(const std::string& fname, WritableFile** result) override {
            int fd = open(fname.c_str(), O_APPEND | O_WRONLY | O_CREAT | OPEN_BASE_FLAGS, 0644);
            if (fd < 0) {
                *result = nullptr;
                return posix_error(fname, errno);
            }
            *result = new PosixWritableFile(fname, fd);
            return Status::OK();
        }

        bool file_exists(const std::string &fname) override {
            return access(fname.c_str(), F_OK) == 0;
        }

        Status get_children(const std::string &dirname, std::vector<std::string> *result) override {
            result->clear();
            DIR *dir = opendir(dirname.c_str());
            if (dir == nullptr) {
                return posix_error(dirname, errno);
            }
            // loop each file in dir
            struct dirent *entry;
            while ((entry = readdir(dir)) != nullptr) {
                result->emplace_back(entry->d_name);
            }
            closedir(dir);
            return Status::OK();
        }

        Status remove_file(const std::string &fname) override {
            if (unlink(fname.c_str()) != 0) {
                return posix_error(fname, errno);
            }
            return Status::OK();
        }

        Status create_dir(const std::string &dirname) override {
            if (mkdir(dirname.c_str(), 0755) != 0) {
                return posix_error(dirname, errno);
            }
            return Status::OK();
        }

        Status remove_dir(const std::string &dirname) override {
            if (rmdir(dirname.c_str()) != 0) {  // dir must be empty
                return posix_error(dirname, errno);
            }
            return Status::OK();
        }

        Status get_file_size(const std::string &fname, uint64_t *file_size) override {
            struct stat file_stat;
            if (stat(fname.c_str(), &file_stat) != 0) {
                *file_size = 0;
                return posix_error(fname, errno);
            }
            *file_size = file_stat.st_size;
            return Status::OK();
        }

        Status rename_file(const std::string &src, const std::string &target) override {
            if(std::rename(src.c_str(), target.c_str()) != 0) {
                return posix_error(src, errno);
            }
            return Status::OK();
        }

        Status lock_file(const std::string &fname, FileLock **lock) override {
            // try open file
            int fd = open(fname.c_str(), O_RDWR | O_CREAT | OPEN_BASE_FLAGS, 0644);
            if (fd < 0) {
                *lock = nullptr;
                return posix_error(fname, errno);
            }
            // try register lock file
            if (!locks.insert(fname)) {
                close(fd);
                return Status::IOError("lock " + fname, "already held by process");
            }
            // lock file using fcntl
            if (lock_or_unlock(fd, true) == -1) {
                int lock_errno = errno;     // save errno before close()
                close(fd);
                locks.remove(fname);
                return posix_error("lock " + fname, lock_errno);
            }
            // ok
            *lock = new PosixFileLock(fd, fname);
            return Status::OK();
        }

        Status unlock_file(FileLock *lock) override {
            PosixFileLock *posix_lock = static_cast<PosixFileLock*>(lock);
            int fd = posix_lock->get_fd();
            const std::string &fname = posix_lock->get_filename();
            if (lock_or_unlock(fd, false) == -1) {
                return posix_error("unlock " + fname, errno);
            }
            locks.remove(fname);
            close(fd);
            delete posix_lock;
            return Status::OK();
        }

        void schedule(void (*function)(void *arg), void *arg) override {
            std::cout << "PosixEnv::schedule() not implemented" << std::endl;
        }
        void start_thread(void (*function)(void *arg), void *arg) override {
            std::cout << "PosixEnv::start_thread() not implemented" << std::endl;
        }

        Status get_test_dir(std::string *path) override {
            const char *env = getenv("TEST_TMPDIR");
            if (env && env[0] != '\0') {
                *path = env;
            } else {
                char buf[100];
                snprintf(buf, sizeof(buf), "/tmp/stackdbtest-%d", geteuid());
                *path = buf;
            }
            create_dir(*path);  // ignore status since dir may already exist
            return Status::OK();
        }

        Status new_logger(const std::string &fname, Logger **result) override {
            *result = nullptr;
            std::cout << "PosixEnv::new_logger() not implemented" << std::endl;
            return Status::NotSupported("PosixEnv::new_logger()");
        }

        uint64_t now_micros() override {
            const uint64_t usecs = 1000000; // miscro-seconds in one second
            struct timeval tv;
            gettimeofday(&tv, nullptr);
            return tv.tv_sec * usecs + tv.tv_usec;
        }

        void sleep_for_microseconds(int micros) override {
            std::this_thread::sleep_for(std::chrono::microseconds(micros));
        }

    private:
        // helper for lock_file() & unlock_file()
        int lock_or_unlock(int fd, bool lock) {
            errno = 0;
            struct flock file_lock;
            std::memset(&file_lock, 0, sizeof(file_lock));
            file_lock.l_type = (lock ? F_WRLCK : F_UNLCK);
            file_lock.l_whence = SEEK_SET;
            file_lock.l_start = 0;
            file_lock.l_len = 0;    // lock/unlock whole file
            return fcntl(fd, F_SETLK, &file_lock);
        }

        int max_mmaps() { return DEFAULT_MMAP_LIMIT; }
        int max_open_fds() {
            if (config_read_fd_limit >= 0) {
                return config_read_fd_limit;
            }
            struct rlimit rlim;
            if (getrlimit(RLIMIT_NOFILE, &rlim)) {          // non-zero indicates error, hard-coded default
                config_read_fd_limit = 50;
            } else if (rlim.rlim_cur == RLIM_INFINITY) {    // set to max int possible if infinity
                config_read_fd_limit = std::numeric_limits<int>::max();
            } else {
                config_read_fd_limit = rlim.rlim_cur / 5;   // allow 20% available fds
            }
            return config_read_fd_limit;
        }

        // below are thread-safe 

        // tracks files locked by PosixEnv::lock_file()
        // the table keep a separate set instead of relying on fcntl(F_SETLK) because
        // fcntl(F_SETLK) does not provide protection against multiple uses from the
        // same process. 
        class PosixLockTable {
        public:
            bool insert(const std::string &fname) {
                // LOCK
                bool succeeded = locked_files.insert(fname).second;
                // UNLOCK
                return succeeded;
            }
            void remove(const std::string &fname) {
                // LOCK
                locked_files.erase(fname);
                // UNLOCK
            }
        private:
            std::set<std::string> locked_files;
        } locks;

        Limiter mmap_limiter;
        Limiter fd_limiter; 
    };
    // interface to get PosixEnv singleton
    static Env *singleton = new PosixEnv();
    Env *Env::get_default() {
        assert(singleton != nullptr);
        return singleton;
    }
} // namespace stackdb

#ifndef STACKDB_ENV_H
#define STACKDB_ENV_H

#include <string>
#include <vector>
#include "stackdb/status.h"

// An Env is an interface used by the leveldb implementation to access
// operating system functionality like the filesystem etc.  Callers
// may wish to provide a custom Env object when opening a database to
// get fine gain control; e.g., to rate limit file system operations.
//
//  all Envs are safe for concurrent thread accesses without sync
//  NEED: support for Windows OS env

namespace stackdb {
    class SequentialFile;       // forward class declarations...
    class RandomAccessFile;
    class WritableFile;
    class FileLock;
    class Logger;

    class Env {
    public:
        Env() = default;
        Env(const Env &) = delete;
        virtual ~Env() = default;

        Env& operator=(const Env&) = delete;

        static Env *get_default();      // currently defined only for posix in env_posix.cpp

        // Env interfaces
        virtual Status new_sequential_file(const std::string& fname, SequentialFile** result) = 0;      // result can be accessed at one time
        virtual Status new_random_access_File(const std::string& fname, RandomAccessFile** result) = 0; // result can be accessed concurrently
        virtual Status new_writable_file(const std::string& fname, WritableFile** result) = 0;          // one time
        virtual Status new_appendable_file(const std::string& fname, WritableFile** result) {           // one time
            return Status::NotSupported("new_appendable_file", fname);                                  // not supported by default
        }
        virtual bool file_exists(const std::string &fname) = 0;                                         // return true if fname exists
        virtual Status get_children(const std::string &dirname, std::vector<std::string> *result) = 0;      // store filenames under dir in *result
        virtual Status remove_file(const std::string &fname) = 0;                                       // delete file. no delete_file as in leveldb
        virtual Status create_dir(const std::string &dirname) = 0;                                      // create dir
        virtual Status remove_dir(const std::string &dirname) = 0;                                      // delete dir. no delete_dir as in leveldb
        virtual Status get_file_size(const std::string &fname, uint64_t *file_size) = 0;                // fetch file size
        virtual Status rename_file(const std::string &src, const std::string &target) = 0;              // rename file src to target

        virtual Status lock_file(const std::string &fname, FileLock **lock) = 0;                        // prevent accesses to same db. unlock() to release lock. no wait if failed
        virtual Status unlock_file(FileLock *lock) = 0;                                                 // release hold lock returned by lock_file() and not already unlocked

        virtual void schedule(void (*function)(void *arg), void *arg) = 0;          // arrange to run function once in a background thread.
        virtual void start_thread(void (*function)(void *arg), void *arg) = 0;      // start a new thread to run function. destroyed when function returned

        virtual Status get_test_dir(std::string *path) = 0;                         // set path to a tempory dir for testing
        virtual Status new_logger(const std::string &fname, Logger **result) = 0;   // create and return a log file for storing messages
        virtual uint64_t now_micros() = 0;                                          // current micro-seconds since xxx. 1 second = 100w micro
        virtual void sleep_for_microseconds(int micros) = 0;                        // sleep/delay thread for micro-seconds
    };

    // a file abstraction for reading sequentially through a file    
    class SequentialFile {
    public:
        SequentialFile() = default;
        SequentialFile(const SequentialFile &) = delete;
        SequentialFile& operator=(const SequentialFile&) = delete;
        virtual ~SequentialFile() = default;
        // interfaces
        virtual Status read(size_t n, Slice* result, char* scratch) = 0;    // read up to n bytes.
        virtual Status skip(uint64_t n) = 0;    // skip n bytes
    };

    // a file abstraction for randomly reading the contents of a file.
    class RandomAccessFile {
    public:
        RandomAccessFile() = default;
        RandomAccessFile(const RandomAccessFile&) = delete;
        RandomAccessFile& operator=(const RandomAccessFile&) = delete;
        virtual ~RandomAccessFile() = default;
        // interfaces
        virtual Status read(uint64_t offset, size_t n, Slice* result, char* scratch) const = 0;   // read up to n bytes at offset
    };
    // a file abstraction for sequential writing. The implementation must provide
    // buffering since callers may append small fragments at a time to the file.
    class WritableFile {
    public:
        WritableFile() = default;
        WritableFile(const WritableFile&) = delete;
        WritableFile& operator=(const WritableFile&) = delete;
        virtual ~WritableFile() = default;
        // interfaces
        virtual Status append(const Slice& data) = 0;
        virtual Status close() = 0;                     // close flushes internal buf, and closes fd
        virtual Status flush() = 0;                     // flush flushes internal buf
        virtual Status sync() = 0;                      // sync flushes buf and underlying system buf
    };  

    // an interface for writing log messages.
    class Logger {
    public:
        Logger() = default;
        Logger(const Logger&) = delete;
        Logger& operator=(const Logger&) = delete;
        virtual ~Logger() = default;
        // interface. write an entry to the log file with the specified format.
        virtual void logv(const char *format, va_list ap) = 0;
    };

    // identifies a locked file.
    class FileLock {
    public:
        FileLock() = default;
        FileLock(const FileLock&) = delete;
        FileLock& operator=(const FileLock&) = delete;
        virtual ~FileLock() = default;
    };

    // log the specified data to *info_log if info_log is non-null.
    void log(Logger* info_log, const char* format, ...);

    // a utility routine: write "data" to the named file.
    Status write_string_to_file(Env* env, const Slice& data, const std::string& fname);
    Status write_string_to_file_sync(Env *env, const Slice &data, const std::string &fname);
    // a utility routine: read contents of named file into *data
    Status read_file_to_string(Env* env, const std::string& fname, std::string* data);
} // namespace stackdb

#endif

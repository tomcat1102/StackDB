#include <cstdarg>
#include "stackdb/env.h"

namespace stackdb {

void logv(Logger* info_log, const char* format, ...) {
    if (info_log != nullptr) {
        va_list ap;

        va_start(ap, format);
        info_log->logv(format, ap);
        va_end(ap);
    }
}

static Status do_write_string_to_file(Env* env, const Slice& data, const std::string& fname, bool should_sync) {
    WritableFile* file;
    Status s = env->new_writable_file(fname, &file);
    if (!s.ok()) return s;

    s = file->append(data);
    if (s.ok() && should_sync) s = file->sync();
    if (s.ok()) s = file->close();

    delete file;    // will auto-close if we did not successfully close above
    if (!s.ok()) env->remove_file(fname);
    return s;
}
Status write_string_to_file(Env* env, const Slice& data, const std::string& fname) {
  return do_write_string_to_file(env, data, fname, false);
}
Status write_string_to_file_sync(Env* env, const Slice& data, const std::string& fname) {
  return do_write_string_to_file(env, data, fname, true);
}

Status read_file_to_string(Env* env, const std::string& fname, std::string* data) {
    data->clear();
    SequentialFile* file;
    Status s = env->new_sequential_file(fname, &file);
    if (!s.ok()) return s;

    // each time try read 8192 bytes
    const int BUFFSER_SIZE = 8192;
    char* space = new char[BUFFSER_SIZE];
    while (true) {          
        Slice fragment;
        s = file->read(BUFFSER_SIZE, &fragment, space);
        if (!s.ok()) break;

        data->append(fragment.data(), fragment.size());
        if (fragment.empty()) break;
    }

    delete[] space;
    delete file;
    return s;
}

}


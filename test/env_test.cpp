#include "stackdb/env.h"
#include "util/random.h"
#include "test_util.h"
using namespace stackdb;

int main() {

    Env *env = Env::get_default();
    // test read & write
    {
        std::string test_dir;
        assert(env->get_test_dir(&test_dir).ok());
        std::string file_path = test_dir + "/open_on_read.txt";
        // create test file
        WritableFile *writable_file;
        assert(env->new_writable_file(file_path, &writable_file).ok());
        
        // fill the file with data from a sequence of randomly sized writes
        Random rnd(Random::time_seed());
        const size_t data_size = 10 * 1024 * 1024; // 10MB
        std::string written;
        while (written.size() < data_size) {
            int len = rnd.skewed(18); // skewed range [0, 2^18 - 1]
            std::string str;
            test::random_string(rnd, len, str);
            writable_file->append(str);
            if (rnd.one_in(10)) {
                assert(writable_file->flush().ok());
            }
            written += str; // append for later check
        }
        assert(writable_file->sync().ok());
        assert(writable_file->close().ok());
        delete writable_file;

        // check written file size
        uint64_t file_size;
        env->get_file_size(file_path, &file_size);
        assert(written.size() == file_size);

        // read the file data with a sequence of randomly sized reads
        SequentialFile *seq_file;
        assert(env->new_sequential_file(file_path, &seq_file).ok());
        std::string readed;
        std::string scratch;
        while (readed.size() < written.size()) {
            int len = std::min<int>(rnd.skewed(18), written.size() - readed.size());
            scratch.resize(std::max(len, 1));  // at least 1 so &scratch[0] is legal
            Slice read;
            assert(seq_file->read(len, &read, &scratch[0]).ok());
            assert(read.size() <= (size_t)len);
            readed.append(read.data(), read.size());
        }
        assert(readed == written);
        delete seq_file;
    }
    // NEED: implement schedule() and start_thread() for tests: run immediately, run many, start thread.
    {
        // ...
    }
    // test open non exsistent file
    {
            std::string test_dir;
            assert(env->get_test_dir(&test_dir).ok());

            std::string non_existent_file = test_dir + "/non_existent_file";
            assert(!env->file_exists(non_existent_file));

            RandomAccessFile* random_access_file;
            assert(env->new_random_access_file(non_existent_file, &random_access_file).is_not_found());

            SequentialFile* sequential_file;
            assert(env->new_sequential_file(non_existent_file, &sequential_file).is_not_found());
    }
    // test on reopen writable file
    {
        std::string test_dir;
        assert(env->get_test_dir(&test_dir).ok());
        std::string test_file_name = test_dir + "/reopen_writable_file.txt";
        env->remove_file(test_file_name);   // ignore status

        WritableFile* writable_file;
        assert(env->new_writable_file(test_file_name, &writable_file).ok());
        std::string data("hello world!");
        assert(writable_file->append(data).ok());
        assert(writable_file->close().ok());
        delete writable_file;

        assert(env->new_writable_file(test_file_name, &writable_file).ok());
        data = "42";
        assert(writable_file->append(data).ok());
        assert(writable_file->close().ok());
        delete writable_file;

        assert(read_file_to_string(env, test_file_name, &data).ok());
        assert(data == "42");
        env->remove_file(test_file_name);
    }
    // test on reopen appendable file
    {
        std::string test_dir;
        assert(env->get_test_dir(&test_dir).ok());
        std::string test_file_name = test_dir + "/reopen_appendable_file.txt";
        env->remove_file(test_file_name);

        WritableFile* appendable_file;
        assert(env->new_appendable_file(test_file_name, &appendable_file).ok());
        std::string data("hello world!");
        assert(appendable_file->append(data).ok());
        assert(appendable_file->close().ok());
        delete appendable_file;

        assert(env->new_appendable_file(test_file_name, &appendable_file).ok());
        data = "42";
        assert(appendable_file->append(data).ok());
        assert(appendable_file->close().ok());
        delete appendable_file;

        assert(read_file_to_string(env, test_file_name, &data).ok());
        assert(data == "hello world!42");
        env->remove_file(test_file_name);
    }
    return 0;
}

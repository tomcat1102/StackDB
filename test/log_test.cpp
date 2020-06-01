#include "stackdb/env.h"
#include "db/dbformat.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "util/random.h"
#include "util/crc32c.h"
#include "util/coding.h"

using namespace stackdb;
using namespace stackdb::log;

// help functions and classes for tests in main()

// construct a string of length n out of supplied partial string.
static std::string big_string(const std::string& partial_string, size_t n) {
    std::string result;
    while (result.size() < n) {
        result.append(partial_string);
    }
    result.resize(n);
    return result;
}
// construct a string from a number
static std::string number_string(int n) {
    char buf[50];
    snprintf(buf, sizeof(buf), "%d.", n);
    return std::string(buf);
}
// construct a skewed potentially long string
static std::string random_skewed_string(int i, Random &rnd) {
  return big_string(number_string(i), rnd.skewed(17));
}

class LogTest {
public:
    LogTest() : reading(false), 
        writer(new Writer(&dest)),
        reader(new Reader(&source, &report, true /*checksum*/, 0 /*initial_offset*/)) {}
    ~LogTest() {
        delete writer;
        delete reader;
    }

    void reopen_for_append() {
        delete writer;
        writer = new Writer(&dest, dest.contents.size());
    }  

    void write(const std::string& msg) {
        assert(!reading);       // "Write() after starting to read";
        writer->add_record(Slice(msg));
    }
    size_t written_bytes() const { return dest.contents.size(); }      

    std::string read() {
        if (!reading) {
            reading = true;
            source.contents = Slice(dest.contents);
        }
        std::string scratch;
        Slice record;
        if (reader->read_record(&record, &scratch)) {
            return record.to_string();
        } else {
            return "EOF";
        }
    }
    void increment_byte(int offset, int delta) { dest.contents[offset] += delta; }
    void set_byte(int offset, char new_byte) { dest.contents[offset] = new_byte; }    
    void shrink_size(int bytes) { dest.contents.resize(dest.contents.size() - bytes); }

    // compute crc of type/len/data
    void fix_checksum(int header_offset, int len) {
        uint32_t crc = crc32c::value(&dest.contents[header_offset + 6], 1 + len);
        crc = crc32c::mask(crc);
        encode_fixed_32(&dest.contents[header_offset], crc);
    }

    void force_error() { source.force_error = true; }
    size_t dropped_bytes() const { return report.dropped_bytes; }
    std::string report_message() const { return report.message; }

    // returns OK iff recorded error message contains "msg"
    std::string match_error(const std::string& msg) const {
        if (report.message.find(msg) == std::string::npos) {
            return report.message;
        } else {
            return "OK";
        }
    }

    void write_init_offset_log() {
        for (int i = 0; i < num_initial_offset_records; i++) {
            std::string record(initial_offset_record_sizes[i], static_cast<char>('a' + i));
            write(record);
        }
    }

    void start_reading_at(uint64_t init_offset) {
        delete reader;
        reader = new Reader(&source, &report, true /*checksum*/, init_offset);
    }

    void check_offset_past_end_returns_no_records(uint64_t offset_past_end) {
        write_init_offset_log();
        reading = true;
        source.contents = Slice(dest.contents);
        Reader* offset_reader = new Reader(&source, &report, true /*checksum*/,
                                    written_bytes() + offset_past_end);
        Slice record;
        std::string scratch;
        assert(!offset_reader->read_record(&record, &scratch));
        delete offset_reader;
    }

    void check_initial_offset_record(uint64_t initial_offset, int expected_record_offset) {
        write_init_offset_log();
        reading = true;
        source.contents = Slice(dest.contents);
        Reader* offset_reader = new Reader(&source, &report, true /*checksum*/, initial_offset);

        // read all records from expected_record_offset through the last one.
        assert(expected_record_offset < num_initial_offset_records);
        for (; expected_record_offset < num_initial_offset_records; ++expected_record_offset) {
            Slice record;
            std::string scratch;

            assert(offset_reader->read_record(&record, &scratch));
            assert(initial_offset_record_sizes[expected_record_offset] == record.size());
            assert(initial_offset_last_record_offsets[expected_record_offset]
                    == offset_reader->get_last_record_offset());
            assert((char)('a' + expected_record_offset) == record.data()[0]);
        }
        delete offset_reader;
    }    

private:
    class StringDest : public WritableFile {
    public:
        Status close() override { return Status::OK(); }
        Status flush() override { return Status::OK(); }
        Status sync() override { return Status::OK(); }
        Status append(const Slice& slice) override {
            contents.append(slice.data(), slice.size());
            return Status::OK(); 
        }
        std::string contents;
    };

    class StringSource : public SequentialFile {
    public:
        StringSource() : force_error(false), returned_partial(false) {}
        Status read(size_t n, Slice* result, char* scratch) override {
            assert(!returned_partial); //"must not Read() after eof/error";
            if (force_error) {
                force_error = false;
                returned_partial = true;
                return Status::Corruption("read error");
            }
            if (contents.size() < n) {
                n = contents.size();
                returned_partial = true;
            }
            *result = Slice(contents.data(), n);
            contents.remove_prefix(n);
            return Status::OK();
        }
        Status skip(uint64_t n) override {
            if (n > contents.size()) {
                contents.clear();
                return Status::NotFound("in-memory file skipped past end");
            }
            contents.remove_prefix(n);
            return Status::OK();
        }
        Slice contents;
        bool force_error;
        bool returned_partial;
    };

    class ReportCollector : public Reader::Reporter {
    public:
        ReportCollector() : dropped_bytes(0) {}
        void corruption(size_t bytes, const Status& status) override {
            dropped_bytes += bytes;
            message.append(status.to_string());
        }
        size_t dropped_bytes;
        std::string message;
    };
private:
    // Record metadata for testing initial offset functionality
    static size_t initial_offset_record_sizes[];
    static uint64_t initial_offset_last_record_offsets[];
    static int num_initial_offset_records;

    StringDest dest;
    StringSource source;
    ReportCollector report;
    bool reading;
    Writer* writer;
    Reader* reader;    
};

size_t LogTest::initial_offset_record_sizes[] = {
    10000,                          // two sizable records in first block
    10000,
    2 * BLOCK_SIZE - 1000,          // span three blocks
    1,
    13716,                          // consume all but two bytes of block 3.
    BLOCK_SIZE - HEADER_SIZE,       // consume the entirety of block 4.
};
uint64_t LogTest::initial_offset_last_record_offsets[] = {
    0,
    HEADER_SIZE + 10000,
    2 * (HEADER_SIZE + 10000),
    2 * (HEADER_SIZE + 10000) + (2 * BLOCK_SIZE - 1000) + 3 * HEADER_SIZE,
    2 * (HEADER_SIZE + 10000) + (2 * BLOCK_SIZE - 1000) + 3 * HEADER_SIZE +
        HEADER_SIZE + 1,
    3 * BLOCK_SIZE,
};
int LogTest::num_initial_offset_records =
    sizeof(LogTest::initial_offset_last_record_offsets) / sizeof(uint64_t);

int main() {
    // suppress used warning
    Random rnd(1);
    random_skewed_string(1, rnd);

    // test empty
    {
        LogTest logger;
        assert(logger.read() == "EOF");
    }
    // test read & write
    {
        LogTest logger;
        logger.write("foo");
        logger.write("bar");
        logger.write("");
        logger.write("xxxx");
        assert(logger.read() == "foo");
        assert(logger.read() == "bar");
        assert(logger.read() == "");
        assert(logger.read() == "xxxx");
        assert(logger.read() == "EOF");
        assert(logger.read() == "EOF");  // Make sure reads at eof work
    }
    // test many blocks
    {
        LogTest logger;
        for (int i = 0; i < 100000; i++) {
            logger.write(number_string(i));
        }
        for (int i = 0; i < 100000; i++) {
            assert(logger.read() == number_string(i));
        }
        assert(logger.read() == "EOF");
        assert(logger.read() == "EOF");
    }
    // test fragmentation
    {
        LogTest logger;
        logger.write("small");
        logger.write(big_string("medium", 50000));
        logger.write(big_string("large", 100000));
        assert(logger.read() == "small");
        assert(logger.read() == big_string("medium", 50000));
        assert(logger.read() == big_string("large", 100000));
        assert(logger.read() == "EOF");
    }
    // test marginal trailer
    {
        LogTest logger;
        // make a trailer that is exactly the same length as an empty record.
        const int n = BLOCK_SIZE - 2 * HEADER_SIZE;
        logger.write(big_string("foo", n));
        assert(logger.written_bytes() ==  BLOCK_SIZE - HEADER_SIZE);
        logger.write("");
        logger.write("bar");
        assert(logger.read() == big_string("foo", n));
        assert(logger.read() == "");
        assert(logger.read() == "bar");
        assert(logger.read() == "EOF");
    }
    // test marginal trailer 2 
    {
        LogTest logger;
        const int n = BLOCK_SIZE - 2 * HEADER_SIZE;
        logger.write(big_string("foo", n));
        assert(logger.written_bytes() == BLOCK_SIZE - HEADER_SIZE);
        logger.write("bar");
        assert(logger.read() == big_string("foo", n));
        assert(logger.read() == "bar");
        assert(logger.read() == "EOF");
        assert(logger.dropped_bytes() == 0);
        assert(logger.report_message() == "");
    }
    // test short trailer
    {
        LogTest logger;
        const int n = BLOCK_SIZE - 2 * HEADER_SIZE + 4;
        logger.write(big_string("foo", n));
        assert(logger.written_bytes() == BLOCK_SIZE - HEADER_SIZE + 4);
        logger.write("");
        logger.write("bar");
        assert(logger.read() == big_string("foo", n));
        assert(logger.read() == "");        
        assert(logger.read() == "bar");
        assert(logger.read() == "EOF");
    }
    // test aligned eof
    {
        LogTest logger;
        const int n = BLOCK_SIZE - 2 * HEADER_SIZE + 4;
        logger.write(big_string("foo", n));
        assert(logger.written_bytes() == BLOCK_SIZE - HEADER_SIZE + 4);
        assert(logger.read() == big_string("foo", n));
        assert(logger.read() == "EOF");
    }
    // test open for append
    {
        LogTest logger;
        logger.write("hello");
        logger.reopen_for_append();
        logger.write("world");
        assert(logger.read() == "hello");
        assert(logger.read() == "world");
        assert(logger.read() == "EOF");
    }
    // test random read
    {
        LogTest logger;
        const int N = 500;
        Random write_rnd(301);
        for (int i = 0; i < N; i++) {
            logger.write(random_skewed_string(i, write_rnd));
        }
        Random read_rnd(301);
        for (int i = 0; i < N; i++) {
            assert(logger.read() == random_skewed_string(i, read_rnd));
        }
        assert(logger.read() == "EOF");
    }
    // tests all error paths in log_reader.cpp 
    // test read error
    {
        LogTest logger;
        logger.write("foo");
        logger.force_error();
        assert(logger.read() == "EOF");
        assert(logger.dropped_bytes() == BLOCK_SIZE);
        assert(logger.match_error("read error") == "OK");
    }    
    // test bad 
    {
        LogTest logger;
        logger.write("foo");
        // Type is stored in header[6]
        logger.increment_byte(6, 100);
        logger.fix_checksum(0, 3);
        assert(logger.read() == "EOF");
        assert(logger.dropped_bytes() == 3);
        assert(logger.match_error("unknown record type") == "OK");
    }
    // test trancated trailling record is ignored
    {
        LogTest logger;
        logger.write("foo");
        logger.shrink_size(4);  // drop all payload as well as a header byte
        assert(logger.read() == "EOF");
        // truncated last record is ignored, not treated as an error.
        assert(logger.dropped_bytes() == 0);
        assert(logger.report_message() == "");
    }
    // test bad length
    {
        LogTest logger;
        const int payload_size = BLOCK_SIZE - HEADER_SIZE;
        logger.write(big_string("bar", payload_size));
        logger.write("foo");
        // Least significant size byte is stored in header[4].
        logger.increment_byte(4, 1);
        assert(logger.read() == "foo");
        assert(logger.dropped_bytes() == BLOCK_SIZE);
        assert(logger.match_error("bad record length") == "OK");
    }
    // test ignored bad length at end
    {
        LogTest logger;
        logger.write("foo");
        logger.shrink_size(1);
        assert(logger.read() == "EOF");
        assert(logger.dropped_bytes() == 0);
        assert(logger.report_message() == "");
    }
    // test checksum mismatch
    {
        LogTest logger;
        logger.write("foo");
        logger.increment_byte(0, 10);
        assert(logger.read() == "EOF");
        assert(logger.dropped_bytes() == 10);
        assert(logger.match_error("checksum mismatch") == "OK");
    }
    // test unexpected middle type
    {
        LogTest logger;
        logger.write("foo");
        logger.set_byte(6, MIDDLE_TYPE);
        logger.fix_checksum(0, 3);
        assert(logger.read() == "EOF");
        assert(logger.dropped_bytes() == 3);
        assert(logger.match_error("missing start") == "OK");
    }
    // test unexpected last type
    {
        LogTest logger;
        logger.write("foo");
        logger.set_byte(6, LAST_TYPE);
        logger.fix_checksum(0, 3);
        assert(logger.read() == "EOF");
        assert(logger.dropped_bytes() == 3);
        assert(logger.match_error("missing start") == "OK");
    }
    // test unpexpected full type
    {
        LogTest logger;
        logger.write("foo");
        logger.write("bar");
        logger.set_byte(6, FIRST_TYPE);
        logger.fix_checksum(0, 3);
        assert(logger.read() == "bar");
        assert(logger.read() == "EOF");
        assert(logger.dropped_bytes() == 3);
        assert(logger.match_error("partial record without end") == "OK");
    }
    // test unexpected first type
    {
        LogTest logger;
        logger.write("foo");
        logger.write(big_string("bar", 100000));
        logger.set_byte(6, FIRST_TYPE);
        logger.fix_checksum(0, 3);
        assert(logger.read() == big_string("bar", 100000));
        assert(logger.read() == "EOF");
        assert(logger.dropped_bytes() == 3);
        assert(logger.match_error("partial record without end") == "OK");
    }
    // test ignore missing last
    {
        LogTest logger;
        logger.write(big_string("bar", BLOCK_SIZE));
        // remove the LAST block, including header.
        logger.shrink_size(14);
        assert(logger.read() == "EOF");
        assert(logger.dropped_bytes() == 0);
        assert(logger.report_message() == "");
    }
    // test ignored partial last
    {
        LogTest logger;
        logger.write(big_string("bar", BLOCK_SIZE));
        // cause a bad record length in the LAST block.
        logger.shrink_size(1);
        assert(logger.read() == "EOF");
        assert(logger.dropped_bytes() == 0);
        assert(logger.report_message() == "");
    }
    // test skip into multi record
    {
        // consider a fragmented record: 
        //    first(R1), middle(R1), last(R1), first(R2)
        // if initial_offset points to a record after first(R1) but before first(R2)
        // incomplete fragment errors are not actual errors, and must be suppressed
        // until a new first or full record is encountered.
        LogTest logger;
        logger.write(big_string("foo", 3 * BLOCK_SIZE));
        logger.write("correct");
        logger.start_reading_at(BLOCK_SIZE);
        assert(logger.read() == "correct");
        assert(logger.read() == "EOF");
        assert(logger.dropped_bytes() == 0);
        assert(logger.report_message() == "");
    }
    // test error joins records
    {
        // consider two fragmented records: 
        //    first(R1) last(R1) first(R2) last(R2)
        // where the middle two fragments disappear.  We do not want
        // first(R1),last(R2) to get joined and returned as a valid record.

        LogTest logger;
        // write records that span two blocks
        logger.write(big_string("foo", BLOCK_SIZE));
        logger.write(big_string("bar", BLOCK_SIZE));
        logger.write("correct");

        // wipe the middle block
        for (int offset = BLOCK_SIZE; offset < 2 * BLOCK_SIZE; offset++) {
            logger.set_byte(offset, 'x');
        }

        assert(logger.read() == "correct");
        assert(logger.read() == "EOF");
        size_t dropped = logger.dropped_bytes();
        assert(dropped <= 2 * BLOCK_SIZE + 100);
        assert(dropped >= 2 * BLOCK_SIZE);
    }
    // test read first one start
    {   LogTest logger; logger.check_initial_offset_record(0, 0); } 
    // test read second one off 
    {   LogTest logger; logger.check_initial_offset_record(1, 1); }
    // test read second ten thousand
    {   LogTest logger; logger.check_initial_offset_record(10000, 1); }
    // test read second start 
    {   LogTest logger; logger.check_initial_offset_record(10007, 1); }
    // test read third one off
    {   LogTest logger; logger.check_initial_offset_record(10008, 2); }
    // test read third start 
    {   LogTest logger; logger.check_initial_offset_record(20014, 2); }
    // test read fourth one off
    {   LogTest logger; logger.check_initial_offset_record(20015, 3); }
    // test read fourth first block trailer
    {   LogTest logger; logger.check_initial_offset_record(BLOCK_SIZE - 4, 3); }
    // test read fourth middel block
    {   LogTest logger; logger.check_initial_offset_record(BLOCK_SIZE + 1, 3); }
    // test read fourth last block
    {   LogTest logger; logger.check_initial_offset_record(BLOCK_SIZE * 2 + 1, 3); }
    // test read fourth start
    {   LogTest logger; logger.check_initial_offset_record(
        2 * (HEADER_SIZE + 1000) + (2 * BLOCK_SIZE - 1000) + 3 * HEADER_SIZE, 3); }
    // test read init offset into block padding
    {   LogTest logger; logger.check_initial_offset_record(BLOCK_SIZE * 3 - 3, 5); }
    // test read end
    {   LogTest logger; logger.check_offset_past_end_returns_no_records(0); }
    // test read past end
    {   LogTest logger; logger.check_offset_past_end_returns_no_records(5); }
    return 0;
}


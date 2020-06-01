#ifndef STACKDB_LOG_READER_H
#define STACKDB_LOG_READER_h

#include <string>
#include <cstdint>
#include "stackdb/slice.h"
#include "stackdb/status.h"
#include "db/log_format.h"

namespace stackdb {
    class SequentialFile;   // forward declaration
    
    namespace log {
        class Reader {
        public:
            class Reporter { // interface for reader to report errors
            public:
                virtual ~Reporter() = default;
                // some corruption was detected with approximate "size" bytes dropped
                virtual void corruption(size_t bytes, const Status& status) = 0;
            };

            // create a reader that returns log records from *file.
            //  if reporter not null, it is notified whenever some data is dropped due to detected corruption
            //  if checksum is true, verify checksums TODO: if available?
            //  reader reads firt record at position >= init_offset in the file
            explicit Reader(SequentialFile *file, Reporter *reporter, bool checksum, uint64_t init_offset)
                : file(file), reporter(reporter), checksum(checksum),                   // params
                  backing_block(new char[BLOCK_SIZE]), buffer(), eof(false),            // block & buf
                  last_record_offset(0), buffer_end_offset(0), init_offset(init_offset),// offsets
                  resyncing(init_offset > 0) {}

            ~Reader() { delete[] backing_block; }

            bool read_record(Slice* record, std::string* scratch);  // read next record into *record. 
            uint64_t get_last_record_offset() {                     // returns physical offset of last record returned by read_record()
                return last_record_offset;
            }

        private:
            enum {  // extent two record types: EOF: hit input end, BAD: invalid crc, 0-lenth, below init_offset
                EOF_TYPE = MAX_RECORD_TYPE + 1,
                BAD_TYPE = MAX_RECORD_TYPE + 2
            };
            
            bool skip_to_init_block();          // skips all blocks that are completely before init_offset. 
            unsigned int read_physical_record(Slice* result);           // return type, or one of the preceding special values

            // reports dropped bytes to the reporter. 
            void report_corruption(uint64_t bytes, const char* reason) {
                report_drop(bytes, Status::Corruption(reason));
            }
            void report_drop(uint64_t bytes, const Status& reason) {
                if (reporter != nullptr && 
                    buffer_end_offset - buffer.size() - bytes >= init_offset) {
                    reporter->corruption(static_cast<size_t>(bytes), reason);
                }
            }

        private:
            SequentialFile* const file;
            Reporter* const reporter;
            bool const checksum;

            char* const backing_block;      // each time backs a new block
            Slice buffer;                   // normally covers entire backing_block, unless last block
            bool eof;                       // EOF only if last read() bytes < BLOCK_SIZE

            uint64_t last_record_offset;    // file offset that last record from read_record()
            uint64_t buffer_end_offset;     // file offset that slice buffer end is at
            uint64_t const init_offset;     // file offset to start looking for first record

            // true if resynchronizing after a seek (init_offset > 0).  skip a run of MIDDLE_TYPE and 
            // a LAST_TYPE partial records to find the first logical record after init_offset
            bool resyncing;                 
        };


        
    } // namespace log
}

#endif


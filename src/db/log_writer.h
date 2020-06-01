#ifndef STACKDB_LOG_WRITER_H
#define STACKDB_LOG_WRITER_H

#include <cstdint>
#include "stackdb/status.h"
#include "db/log_format.h"

namespace stackdb {
    class WritableFile; // forward declaration

    namespace log {
        class Writer {
        public:
            // create a writer that will append data to *dest
            // *dest must be initially empty and remain alive for the writer
            explicit Writer(WritableFile *dest);
            Writer(WritableFile *dest, uint64_t dest_length);
            Writer(const Writer&) = delete;
            ~Writer() = default;
            Writer &operator=(const Writer &) = delete;

            Status add_record(const Slice &slice);

        private:
            Status emit_physical_record(RecordType type, const char *ptr, size_t length);
            WritableFile *dest;
            int block_offset;   // current offset in block
            // pre-computed crc32c header values for all supported record typesss
            uint32_t type_crc[MAX_RECORD_TYPE + 1];
        };
    } // namespace log
}

#endif


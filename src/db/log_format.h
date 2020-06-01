#ifndef STACKDB_LOG_FORMAT_H
#define STACKDB_LOG_FORMAT_H

namespace stackdb {
    namespace log {
        // no class scoped
        enum RecordType {
            // zero is reserved for preallocated files
            ZERO_TYPE = 0,
            // full record
            FULL_TYPE = 1,
            // record fragments
            FIRST_TYPE = 2,
            MIDDLE_TYPE = 3,
            LAST_TYPE = 4
        };
        const int MAX_RECORD_TYPE = LAST_TYPE;

        const int BLOCK_SIZE = 32 * 1024;
        const int HEADER_SIZE = 4 + 2 + 1; // checksum 4 + length 2 + type 1
    }
}

#endif


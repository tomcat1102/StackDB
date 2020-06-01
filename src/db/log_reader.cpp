#include "stackdb/env.h"
#include "db/log_format.h"
#include "db/log_reader.h"
#include "util/crc32c.h"
#include "util/coding.h"

#include <iostream>

namespace stackdb {
    namespace log {
        bool Reader::skip_to_init_block() {
            size_t within_block_offset = init_offset % BLOCK_SIZE;
            uint64_t init_block_offset = init_offset - within_block_offset;

            // switch to next block, since no record in a block is after trailer offset 
            if (within_block_offset > BLOCK_SIZE - 6) {
                init_block_offset += BLOCK_SIZE;
            }

            buffer_end_offset = init_block_offset;

            // Skip to start of first block that can contain the initial record :)
            if (buffer_end_offset > 0) {
                Status skip_status = file->skip(init_block_offset);
                if (!skip_status.ok()) {
                    report_drop(init_block_offset, skip_status);
                    return false;
                }
            }
            return true;
        }

        bool Reader::read_record(Slice* record, std::string* scratch) {
            // skip to init block that contains first record, if not skipped
            if (last_record_offset < init_offset) {
                if (!skip_to_init_block()) return false;
            }

            scratch->clear();
            record->clear();

            Slice fragment;
            bool in_fragmented_record = false;
            uint64_t prospective_record_offset;             // record offset of logical record

            while (true) {
                const unsigned int record_type = read_physical_record(&fragment);
                uint64_t physical_record_offset = buffer_end_offset 
                            - buffer.size() - HEADER_SIZE - fragment.size();

                if (resyncing) {    // effective if init_offset is at block boundaries
                    if (record_type == MIDDLE_TYPE) {
                        continue;
                    } else if (record_type == LAST_TYPE) {
                        resyncing = false;
                        continue;
                    } else {
                        resyncing = false;
                    }
                }

                switch (record_type) {
                    case FULL_TYPE:
                        if (in_fragmented_record) {     // bug block in earlier versions of log::Writer ... 
                            if (!scratch->empty()) {    // report corrupted bytes == scratch->size()
                                report_corruption(scratch->size(), "partial record without end(1)");
                            }
                        }
                        prospective_record_offset = physical_record_offset;
                        scratch->clear();
                        *record = fragment;
                        last_record_offset = prospective_record_offset;
                        return true;

                    case FIRST_TYPE:
                        if (in_fragmented_record) {     // bug block in earlier versions of log::Writer ... 
                            if (!scratch->empty()) {
                                report_corruption(scratch->size(), "partial record without end(2)");
                            }
                        }
                        prospective_record_offset = physical_record_offset;
                        scratch->assign(fragment.data(), fragment.size());
                        in_fragmented_record = true;
                        break;

                    case MIDDLE_TYPE:
                        if (!in_fragmented_record) {
                            report_corruption(fragment.size(), "missing start of fragmented record(1)");
                        } else {
                            scratch->append(fragment.data(), fragment.size());
                        }
                        break;

                    case LAST_TYPE: 
                        if (!in_fragmented_record) {
                            report_corruption(fragment.size(), "missing start of fragmented record(2)");
                        } else {
                            scratch->append(fragment.data(), fragment.size());
                            *record = Slice(*scratch);
                            last_record_offset = prospective_record_offset;
                            return true;
                        }
                        break;

                    case EOF_TYPE:
                        if (in_fragmented_record) { // note writer death before completing record
                            scratch->clear();
                        }
                        return false;

                    case BAD_TYPE:
                        if (in_fragmented_record) {
                            report_corruption(scratch->size(), "error in middle of record");
                            in_fragmented_record = false;   // skip bad record
                            scratch->clear();
                        }
                        break;

                    default: {
                        char buf[40];
                        snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
                        report_corruption((fragment.size() + (in_fragmented_record ? scratch->size() : 0)), buf);
                        in_fragmented_record = false;   // skip unknown record
                        scratch->clear();
                        break;
                    }
                }
            }
            return false;
        }

        unsigned int Reader::read_physical_record(Slice* result) {
            // try refill buffer for next record, if buffer can't fit a header
            while (buffer.size() < HEADER_SIZE) {
                if (eof) {              // no data available. return EOF
                    buffer.clear();
                    return EOF_TYPE;
                }
                buffer.clear();          // skip trailer and read a new block
                Status status = file->read(BLOCK_SIZE, &buffer, backing_block);
                buffer_end_offset += buffer.size();

                if (!status.ok()) {     // treat error as eof.
                    buffer.clear();
                    report_drop(BLOCK_SIZE, status);
                    eof = true;
                    return EOF_TYPE;
                } 
                if (buffer.size() < BLOCK_SIZE) { // set eof if read < BLOCK_SIZE
                    eof = true;
                }
            }
            // now header in buffer. try parse it
            const char *header = buffer.data();
            uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
            uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
            uint32_t length = a | (b << 8);
            uint32_t type = header[6];

            // ensure buffer holds current record
            if (HEADER_SIZE + length > buffer.size()) {
                size_t drop_size = buffer.size();
                buffer.clear();
                if (!eof) {
                    report_corruption(drop_size, "bad record length");
                    return BAD_TYPE;
                }
                return EOF_TYPE;    // no report. assume writer died while writing the record
            }
            // return if empty record. current no support for it
            if (type == ZERO_TYPE && length == 0) {
                buffer.clear();
                return BAD_TYPE;
            }

            // check record crc32
            if (checksum) {
                uint32_t actual_crc = crc32c::unmask(decode_fixed_32(header));
                uint32_t expected_crc = crc32c::value(header + 6, 1 + length);
                if (actual_crc != expected_crc) {
                    size_t drop_size = buffer.size();       // drop rest of buffer
                    buffer.clear();
                    report_corruption(drop_size, "checksum mismatch");
                    return BAD_TYPE;
                }
            }
            // record ok, consume it in buffer
            buffer.remove_prefix(HEADER_SIZE + length);

            // skip physical record that started before init_offset
            if (buffer_end_offset - buffer.size() - HEADER_SIZE - length < init_offset) {
                result->clear();
                return BAD_TYPE;
            }

            *result = Slice(header + HEADER_SIZE, length);
            return type;
        }

    } // namespace log
} // namespace stackdb


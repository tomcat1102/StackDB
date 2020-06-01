#include "stackdb/env.h"
#include "db/log_writer.h"
#include "util/crc32c.h"
#include "util/coding.h"


namespace stackdb {
namespace log {

static void init_type_crc(uint32_t *type_crc) {
    for (int i = 0; i <= MAX_RECORD_TYPE; i ++) { // took 2 hours to debug this
        char type = static_cast<char>(i);
        type_crc[i] = crc32c::value(&type, 1);
    }
}

Writer::Writer(WritableFile* dest) : dest(dest), block_offset(0) {
    init_type_crc(type_crc);
}
Writer::Writer(WritableFile* dest, uint64_t dest_length)
    : dest(dest), block_offset(dest_length % BLOCK_SIZE) {
    init_type_crc(type_crc);
}

Status Writer::add_record(const Slice &slice) {
    const char *ptr = slice.data();
    size_t left = slice.size();
    // Fragment the record if necessary and emit it. do it even for empty record
    Status s;
    bool begin = true;
    do {
        const int block_leftover = BLOCK_SIZE - block_offset;
        assert(block_leftover >= 0);
        // fill left over with zero if the space not enough for header
        if (block_leftover < HEADER_SIZE) {
            if (block_leftover > 0) { // only fill non-empty left over
                dest->append(Slice("\x00\x00\x00\x00\x00\x00", block_leftover));
            }
            block_offset = 0;  // indicate switching to a new block
        }

        // Invariant: we never leave < kHeaderSize bytes in a block.
        assert(BLOCK_SIZE - block_offset - HEADER_SIZE >= 0);

        const size_t avail = BLOCK_SIZE - block_offset - HEADER_SIZE;
        const size_t frag_len = (left < avail) ? left : avail;

        RecordType type;    // determine record type by 'begin' and 'end'
        const bool end = (left == frag_len);
        if (begin && end) {  
            type = FULL_TYPE;
        } else if (begin) {
            type = FIRST_TYPE;
        } else if (end) {
            type = LAST_TYPE;
        } else {
            type = MIDDLE_TYPE;
        }

        s = emit_physical_record(type, ptr, frag_len);
        ptr  += frag_len;
        left -= frag_len;
        begin = false;
    } while (s.ok() && left > 0);
    return s;
}

Status Writer::emit_physical_record(RecordType type, const char *ptr, size_t length) {
    assert(length <= 0xffff);       // must fit in two bytes
    assert(block_offset + HEADER_SIZE + length <= BLOCK_SIZE);

    // form the header: checksum[0-3] | length[4-5] | type[6]
    char buf[HEADER_SIZE];
    buf[4] = static_cast<char>(length & 0xff);
    buf[5] = static_cast<char>(length >> 8);
    buf[6] = static_cast<char>(type);

    // compute crc of record type and payload. mast the computed crc for 
    // storage since it's hard to compute crc of string containing embeded crc
    uint32_t crc = crc32c::extend(type_crc[type], ptr, length);
    crc = crc32c::mask(crc);  
    encode_fixed_32(buf, crc);

    // write header and payload
    Status s = dest->append(Slice(buf, HEADER_SIZE));
    if (s.ok()) {
        s = dest->append(Slice(ptr, length));
        if (s.ok()) {
            s = dest->flush();
        }
    }
    block_offset += HEADER_SIZE + length;
    return s;
}

}
}


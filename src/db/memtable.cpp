#include "db/memtable.h"
#include "util/coding.h"

namespace stackdb {

// get slice representaion of internal key from memtable key
Slice get_length_prefixed_slice(const char *data) {
    uint32_t size;
    const char *p = data;
    p = get_varint_32_ptr(p, p + 5, &size);   // assume p is not corrupted
    return Slice(p, size);
}

int MemTable::KeyComparator::operator()(const char *a, const char *b) const {
    Slice sa = get_length_prefixed_slice(a);
    Slice sb = get_length_prefixed_slice(b);
    return comparator.compare(sa, sb);
}

// internal memtable iterator implementation for MemTable::new_iterator()
class MemTableIterator: public Iterator{
public:
    explicit MemTableIterator(MemTable::Table *table): iter(table) {}
    MemTableIterator(const MemTableIterator&) = delete;
    ~MemTableIterator() override = default;
    MemTableIterator &operator=(const MemTableIterator &) = delete;
    // implements interface Iterator
    bool valid() const override { return iter.valid(); }
    void seek_to_first() override { iter.seek_to_first(); }
    void seek_to_last() override { iter.seek_to_last(); }
    void seek(const Slice &key) override { iter.seek(encode_key(&spirit, key)); }
    void next() override { iter.next(); }
    void prev() override { iter.prev(); }
    Slice key() override { return get_length_prefixed_slice(iter.key()); }
    Slice value() override {    // remember the format: key_slice|val_slice
        Slice key_slice = get_length_prefixed_slice(iter.key());
        return get_length_prefixed_slice(key_slice.data() + key_slice.size());
    }
    Status status() const override { return Status::OK(); }

private:
    // iter.seek() ultimately uses MemTable::KeyComparator which takes 
    // a memtable key. so here we encode the internal key to memtable key
    const char *encode_key(std::string *spirit, const Slice& target) {
        spirit->clear();
        append_varint_32(spirit, target.size());
        spirit->append(target.data(), target.size());
        return spirit->data();
    }

    MemTable::Table::Iterator iter; // = SkipList::Iterator
    std::string spirit;             // for passing to encode_key()
};

// Format of an entry is concatenation of:
//  key_size     : varint32 of internal_key.size()
//  key bytes    : char[internal_key.size()]
//  value_size   : varint32 of value.size()
//  value bytes  : char[value.size()]
void MemTable::add(SeqNum seq, ValType type, const Slice &key, const Slice &value) {
    size_t key_size = key.size();
    size_t val_size = value.size();
    size_t internal_key_size = key_size + 8;
    size_t encoded_len = varint_length(internal_key_size) + internal_key_size
                       + varint_length(val_size) + val_size;

    char *buf = arena.allocate(encoded_len);
    char *p = encode_varint_32(buf, internal_key_size);
    std::memcpy(p, key.data(), key_size);
    p += key.size();
    encode_fixed_64(p, (seq << 8 | static_cast<int>(type))); // we have pack_seq_and_type(), however it's not accessible
    p += 8;
    p = encode_varint_32(p, val_size);
    std::memcpy(p, value.data(), val_size);

    assert(p + val_size == buf + encoded_len);

    table.insert(buf);
}
// entry format:
//    key_len  varint32
//    userkey  char[key_len]
//    seqtype  uint64
//    val_len  varint32
//    value    char[val_len]
// If valid, check it belongs to same user key.  We do not check the
// sequence number since the Seek() call above should have skipped
// all entries with overly large sequence numbers.
bool MemTable::get(const LookupKey &key, std::string *value, Status *s) {
    Slice mem_key = key.memtable_key();
    
    Table::Iterator iter(&table);
    iter.seek(mem_key.data());

    if (iter.valid()) {
        const char *entry = iter.key();
        uint32_t key_len;
        const char *key_ptr = get_varint_32_ptr(entry, entry + 5, &key_len);
        // invoke user-defined comparator to compare user keys
        if (comparator.comparator.user_comparator()->compare(
                Slice(key_ptr, key_len - 8), key.user_key()) == 0) {

            // if same user key, check type
            uint64_t seq_type = decode_fixed_64(key_ptr + key_len - 8);
            Slice val;  // cannot define it under case label !!

            switch(static_cast<ValType>(seq_type && 0xff)) {
            case ValType::VALUE:
                val = get_length_prefixed_slice(key_ptr + key_len);
                value->assign(val.data(), val.size());
                return true;
            case ValType::DELETION:
                *s = Status::NotFound(Slice());
                return true;
            }
        }
    }
    return false;
}

} // namespace stackdb
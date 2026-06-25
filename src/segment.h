#pragma once
#include <cstdint>
#include <string>
#include <vector>

// A segment is a pair of files: <base_offset>.log (data) + <base_offset>.index (sparse offset index).
//
// Log record layout: [offset(8)] [timestamp(8)] [key_len(4)] [val_len(4)] [key] [val]
//
// Index entry: [log_offset(8)] [file_position(8)]
// One index entry is written every kIndexInterval records, enabling O(log n) seeks.

struct Record {
    uint64_t    offset    = 0;
    uint64_t    timestamp = 0;
    std::string key;
    std::string value;
};

class Segment {
public:
    static constexpr int      kIndexInterval = 8;
    static constexpr uint64_t kMaxBytes      = 64 * 1024 * 1024; // 64MB

    // Open existing or create new segment starting at base_offset.
    Segment(const std::string& dir, uint64_t base_offset, bool read_only = false);
    ~Segment();

    // Returns the assigned offset, or 0 on failure.
    uint64_t Append(const std::string& key, const std::string& value);

    // Read record at logical offset. Returns false if not found in this segment.
    bool ReadAt(uint64_t offset, Record& out) const;

    // Iterate records starting from logical offset (inclusive).
    // Calls fn(record) for each; stops when fn returns false.
    void Scan(uint64_t from_offset, std::function<bool(const Record&)> fn) const;

    uint64_t BaseOffset()  const { return base_offset_; }
    uint64_t NextOffset()  const { return next_offset_; }
    uint64_t SizeBytes()   const { return log_size_; }
    bool     Full()        const { return log_size_ >= kMaxBytes; }

private:
    std::string dir_;
    uint64_t    base_offset_;
    uint64_t    next_offset_;
    uint64_t    log_size_   = 0;
    int         log_fd_     = -1;
    int         index_fd_   = -1;
    bool        read_only_  = false;
    int         count_      = 0;

    struct IndexEntry { uint64_t offset; uint64_t position; };
    std::vector<IndexEntry> index_; // loaded into memory for reads

    void LoadIndex();
    uint64_t FindPosition(uint64_t offset) const;

    static uint64_t NowMs();
};

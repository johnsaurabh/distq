#pragma once
#include "segment.h"
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

// A partition is an ordered sequence of segments forming an infinite append log.
// The active (last) segment receives writes. Sealed segments are read-only.
// Consumer offsets start at 0 and advance monotonically.
class Partition {
public:
    Partition(const std::string& data_dir, const std::string& topic, int partition_id);

    // Append a message. Returns the assigned offset.
    uint64_t Append(const std::string& key, const std::string& value);

    // Fetch up to max_count records starting at offset. Returns actual count fetched.
    std::vector<Record> Fetch(uint64_t offset, int max_count) const;

    uint64_t NextOffset() const;
    uint64_t StartOffset() const;

private:
    std::string base_dir_;
    mutable std::mutex mu_;
    std::vector<std::unique_ptr<Segment>> segments_;

    void RollSegment();
    Segment* ActiveSegment();
    const Segment* FindSegment(uint64_t offset) const;
};

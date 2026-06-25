#pragma once
#include "broker.h"
#include <string>
#include <unordered_map>
#include <vector>

// Consumer tracks its own offset per partition and reads sequentially.
// In a real cluster, committed offsets are stored on the broker (or in a
// dedicated __consumer_offsets topic) so consumers can restart from where they left off.
class Consumer {
public:
    Consumer(Broker* broker, const std::string& topic, int partition,
             uint64_t initial_offset = 0);

    // Poll up to max_count records. Advances the committed offset.
    std::vector<Record> Poll(int max_count = 100);

    // Commit the current position (e.g., after processing).
    void Commit();

    uint64_t CommittedOffset() const { return committed_offset_; }
    uint64_t CurrentOffset()   const { return current_offset_;   }

private:
    Broker*     broker_;
    std::string topic_;
    int         partition_;
    uint64_t    current_offset_   = 0;
    uint64_t    committed_offset_ = 0;
};

#include "consumer.h"

Consumer::Consumer(Broker* broker, const std::string& topic, int partition,
                   uint64_t initial_offset)
    : broker_(broker), topic_(topic), partition_(partition),
      current_offset_(initial_offset), committed_offset_(initial_offset)
{}

std::vector<Record> Consumer::Poll(int max_count) {
    auto records = broker_->Fetch(topic_, partition_, current_offset_, max_count);
    if (!records.empty())
        current_offset_ = records.back().offset + 1;
    return records;
}

void Consumer::Commit() {
    committed_offset_ = current_offset_;
}

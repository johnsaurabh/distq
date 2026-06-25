#include "broker.h"
#include <stdexcept>

Broker::Broker(const std::string& data_dir) : data_dir_(data_dir) {
    std::filesystem::create_directories(data_dir);
}

void Broker::CreateTopic(const std::string& topic, TopicConfig cfg) {
    std::lock_guard<std::mutex> lk(mu_);
    if (topics_.count(topic)) return; // already exists
    auto& te = topics_[topic];
    te.config = cfg;
    for (int i = 0; i < cfg.num_partitions; ++i)
        te.partitions.emplace_back(std::make_unique<Partition>(data_dir_, topic, i));
}

Broker::TopicEntry& Broker::GetTopic(const std::string& topic) {
    auto it = topics_.find(topic);
    if (it == topics_.end()) throw std::runtime_error("unknown topic: " + topic);
    return it->second;
}

int Broker::PartitionForKey(const std::string& key, int n) {
    // FNV-1a hash for deterministic partition assignment
    uint32_t h = 2166136261u;
    for (unsigned char c : key) { h ^= c; h *= 16777619u; }
    return static_cast<int>(h % static_cast<uint32_t>(n));
}

uint64_t Broker::Produce(const std::string& topic, const std::string& key,
                          const std::string& value) {
    std::lock_guard<std::mutex> lk(mu_);
    auto& te = GetTopic(topic);
    int p = PartitionForKey(key, static_cast<int>(te.partitions.size()));
    return te.partitions[p]->Append(key, value);
}

std::vector<Record> Broker::Fetch(const std::string& topic, int partition,
                                   uint64_t offset, int max_count) {
    std::lock_guard<std::mutex> lk(mu_);
    auto& te = GetTopic(topic);
    if (partition < 0 || partition >= static_cast<int>(te.partitions.size()))
        throw std::out_of_range("invalid partition");
    return te.partitions[partition]->Fetch(offset, max_count);
}

uint64_t Broker::PartitionNextOffset(const std::string& topic, int partition) const {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = topics_.find(topic);
    if (it == topics_.end()) throw std::runtime_error("unknown topic: " + topic);
    return it->second.partitions.at(partition)->NextOffset();
}

int Broker::NumPartitions(const std::string& topic) const {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = topics_.find(topic);
    if (it == topics_.end()) return 0;
    return static_cast<int>(it->second.partitions.size());
}

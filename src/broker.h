#pragma once
#include "partition.h"
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

struct TopicConfig {
    int num_partitions    = 1;
    int replication_factor = 1; // for demo: in-memory replica simulation
};

// The broker owns all partitions for all topics.
// In a real cluster each broker owns a subset; the leader for each partition
// handles all reads/writes for that partition.
class Broker {
public:
    explicit Broker(const std::string& data_dir);

    void CreateTopic(const std::string& topic, TopicConfig cfg = {});

    // Returns the assigned offset. Partitions by hash(key) % num_partitions.
    uint64_t Produce(const std::string& topic, const std::string& key,
                     const std::string& value);

    // Fetch up to max_count records from a specific partition, starting at offset.
    std::vector<Record> Fetch(const std::string& topic, int partition,
                               uint64_t offset, int max_count = 100);

    uint64_t PartitionNextOffset(const std::string& topic, int partition) const;
    int      NumPartitions(const std::string& topic) const;

private:
    std::string data_dir_;
    mutable std::mutex mu_;

    struct TopicEntry {
        TopicConfig config;
        std::vector<std::unique_ptr<Partition>> partitions;
    };
    std::unordered_map<std::string, TopicEntry> topics_;

    TopicEntry& GetTopic(const std::string& topic);
    static int PartitionForKey(const std::string& key, int n);
};

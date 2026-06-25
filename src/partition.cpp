#include "partition.h"
#include <algorithm>
#include <filesystem>
#include <stdexcept>

namespace fs = std::filesystem;

Partition::Partition(const std::string& data_dir, const std::string& topic, int id) {
    base_dir_ = data_dir + "/" + topic + "/p" + std::to_string(id);
    fs::create_directories(base_dir_);

    // Load existing segments from disk (files named <base_offset>.log)
    std::vector<uint64_t> bases;
    for (auto& entry : fs::directory_iterator(base_dir_)) {
        auto name = entry.path().filename().string();
        if (name.size() > 4 && name.substr(name.size() - 4) == ".log") {
            try { bases.push_back(std::stoull(name.substr(0, name.size() - 4))); }
            catch (...) {}
        }
    }
    std::sort(bases.begin(), bases.end());

    for (size_t i = 0; i < bases.size(); ++i) {
        bool ro = (i + 1 < bases.size()); // all but last are read-only
        segments_.emplace_back(std::make_unique<Segment>(base_dir_, bases[i], ro));
    }

    // No existing segments — create first one at offset 0
    if (segments_.empty())
        segments_.emplace_back(std::make_unique<Segment>(base_dir_, 0));
}

Segment* Partition::ActiveSegment() {
    return segments_.back().get();
}

const Segment* Partition::FindSegment(uint64_t offset) const {
    // Binary search: find the segment whose base_offset <= offset < next segment's base
    for (int i = static_cast<int>(segments_.size()) - 1; i >= 0; --i) {
        if (segments_[i]->BaseOffset() <= offset) return segments_[i].get();
    }
    return nullptr;
}

void Partition::RollSegment() {
    uint64_t next = ActiveSegment()->NextOffset();
    segments_.back() = std::make_unique<Segment>(base_dir_, segments_.back()->BaseOffset(), true);
    segments_.emplace_back(std::make_unique<Segment>(base_dir_, next));
}

uint64_t Partition::Append(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lk(mu_);
    if (ActiveSegment()->Full()) RollSegment();
    return ActiveSegment()->Append(key, value);
}

std::vector<Record> Partition::Fetch(uint64_t offset, int max_count) const {
    std::lock_guard<std::mutex> lk(mu_);
    std::vector<Record> results;
    results.reserve(max_count);

    const Segment* seg = FindSegment(offset);
    if (!seg) return results;

    size_t seg_idx = 0;
    for (size_t i = 0; i < segments_.size(); ++i) {
        if (segments_[i].get() == seg) { seg_idx = i; break; }
    }

    while (static_cast<int>(results.size()) < max_count && seg_idx < segments_.size()) {
        segments_[seg_idx]->Scan(offset + results.size(), [&](const Record& r) -> bool {
            results.push_back(r);
            return static_cast<int>(results.size()) < max_count;
        });
        ++seg_idx;
    }
    return results;
}

uint64_t Partition::NextOffset() const {
    std::lock_guard<std::mutex> lk(mu_);
    return segments_.back()->NextOffset();
}

uint64_t Partition::StartOffset() const {
    std::lock_guard<std::mutex> lk(mu_);
    return segments_.front()->BaseOffset();
}

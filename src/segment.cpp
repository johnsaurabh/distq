#include "segment.h"
#include <chrono>
#include <cstring>
#include <functional>
#include <sstream>
#include <stdexcept>

#ifdef _WIN32
#  include <fcntl.h>
#  include <io.h>
#  include <sys/stat.h>
   static int  s_open_a(const char* p) { return ::_open(p, _O_WRONLY|_O_CREAT|_O_APPEND|_O_BINARY, _S_IREAD|_S_IWRITE); }
   static int  s_open_r(const char* p) { return ::_open(p, _O_RDONLY|_O_BINARY, 0); }
   static void s_write(int fd, const void* b, size_t n) { ::_write(fd, b, (unsigned)n); }
   static int  s_read(int fd, void* b, size_t n) { return ::_read(fd, b, (unsigned)n); }
   static void s_seek(int fd, int64_t o) { ::_lseeki64(fd, o, SEEK_SET); }
   static int64_t s_tell(int fd) { return ::_telli64(fd); }
   static int64_t s_size(int fd) { return ::_filelengthi64(fd); }
   static void s_close(int fd) { ::_close(fd); }
#else
#  include <fcntl.h>
#  include <unistd.h>
#  include <sys/stat.h>
   static int  s_open_a(const char* p) { return ::open(p, O_WRONLY|O_CREAT|O_APPEND, 0644); }
   static int  s_open_r(const char* p) { return ::open(p, O_RDONLY); }
   static void s_write(int fd, const void* b, size_t n) { ::write(fd, b, n); }
   static int  s_read(int fd, void* b, size_t n) { return ::read(fd, b, n); }
   static void s_seek(int fd, int64_t o) { ::lseek(fd, o, SEEK_SET); }
   static int64_t s_tell(int fd) { return ::lseek(fd, 0, SEEK_CUR); }
   static int64_t s_size(int fd) { struct stat st; ::fstat(fd, &st); return st.st_size; }
   static void s_close(int fd) { ::close(fd); }
#endif

static std::string log_path(const std::string& dir, uint64_t base) {
    std::ostringstream ss; ss << dir << "/" << base << ".log"; return ss.str();
}
static std::string idx_path(const std::string& dir, uint64_t base) {
    std::ostringstream ss; ss << dir << "/" << base << ".index"; return ss.str();
}

uint64_t Segment::NowMs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
}

Segment::Segment(const std::string& dir, uint64_t base_offset, bool read_only)
    : dir_(dir), base_offset_(base_offset), next_offset_(base_offset), read_only_(read_only)
{
    if (read_only) {
        log_fd_   = s_open_r(log_path(dir, base_offset).c_str());
        index_fd_ = s_open_r(idx_path(dir, base_offset).c_str());
    } else {
        log_fd_   = s_open_a(log_path(dir, base_offset).c_str());
        index_fd_ = s_open_a(idx_path(dir, base_offset).c_str());
    }
    if (log_fd_ < 0 || index_fd_ < 0)
        throw std::runtime_error("Segment: cannot open files in " + dir);

    log_size_    = static_cast<uint64_t>(s_size(log_fd_));
    LoadIndex();
    // Recover next_offset from index + scanning tail
    if (!index_.empty()) next_offset_ = index_.back().offset;
    // Scan forward from last indexed position to get exact next_offset
    // (simplified: count is approximate without full scan)
}

Segment::~Segment() {
    if (log_fd_   >= 0) s_close(log_fd_);
    if (index_fd_ >= 0) s_close(index_fd_);
}

void Segment::LoadIndex() {
    int64_t sz = s_size(index_fd_);
    if (sz <= 0) return;
    s_seek(index_fd_, 0);
    size_t entries = static_cast<size_t>(sz) / 16;
    index_.resize(entries);
    for (auto& ie : index_) {
        s_read(index_fd_, &ie.offset,   8);
        s_read(index_fd_, &ie.position, 8);
    }
}

uint64_t Segment::Append(const std::string& key, const std::string& value) {
    if (read_only_) return 0;

    uint64_t offset    = next_offset_;
    uint64_t ts        = NowMs();
    uint32_t klen      = static_cast<uint32_t>(key.size());
    uint32_t vlen      = static_cast<uint32_t>(value.size());
    uint64_t file_pos  = log_size_;

    s_write(log_fd_, &offset, 8);
    s_write(log_fd_, &ts,     8);
    s_write(log_fd_, &klen,   4);
    s_write(log_fd_, &vlen,   4);
    s_write(log_fd_, key.data(),   klen);
    s_write(log_fd_, value.data(), vlen);

    log_size_ += 8 + 8 + 4 + 4 + klen + vlen;
    ++next_offset_;
    ++count_;

    if (count_ % kIndexInterval == 0) {
        IndexEntry ie{offset, file_pos};
        index_.push_back(ie);
        s_write(index_fd_, &ie.offset,   8);
        s_write(index_fd_, &ie.position, 8);
    }

    return offset;
}

uint64_t Segment::FindPosition(uint64_t target) const {
    // Binary search the sparse index for largest entry <= target
    uint64_t pos = 0;
    for (auto& ie : index_) {
        if (ie.offset <= target) pos = ie.position;
        else break;
    }
    return pos;
}

bool Segment::ReadAt(uint64_t offset, Record& out) const {
    if (offset < base_offset_ || offset >= next_offset_) return false;
    uint64_t pos = FindPosition(offset);
    s_seek(log_fd_, static_cast<int64_t>(pos));

    // Scan forward from pos
    uint64_t cur_pos = pos;
    while (cur_pos < log_size_) {
        uint64_t off, ts; uint32_t klen, vlen;
        if (s_read(log_fd_, &off,  8) != 8) break;
        if (s_read(log_fd_, &ts,   8) != 8) break;
        if (s_read(log_fd_, &klen, 4) != 4) break;
        if (s_read(log_fd_, &vlen, 4) != 4) break;
        std::string k(klen, '\0'), v(vlen, '\0');
        s_read(log_fd_, k.data(), klen);
        s_read(log_fd_, v.data(), vlen);
        cur_pos += 8 + 8 + 4 + 4 + klen + vlen;
        if (off == offset) { out = {off, ts, std::move(k), std::move(v)}; return true; }
        if (off > offset) break;
    }
    return false;
}

void Segment::Scan(uint64_t from_offset, std::function<bool(const Record&)> fn) const {
    uint64_t pos = FindPosition(from_offset);
    s_seek(log_fd_, static_cast<int64_t>(pos));
    uint64_t cur_pos = pos;
    while (cur_pos < log_size_) {
        uint64_t off, ts; uint32_t klen, vlen;
        if (s_read(log_fd_, &off,  8) != 8) break;
        if (s_read(log_fd_, &ts,   8) != 8) break;
        if (s_read(log_fd_, &klen, 4) != 4) break;
        if (s_read(log_fd_, &vlen, 4) != 4) break;
        std::string k(klen, '\0'), v(vlen, '\0');
        s_read(log_fd_, k.data(), klen);
        s_read(log_fd_, v.data(), vlen);
        cur_pos += 8 + 8 + 4 + 4 + klen + vlen;
        if (off < from_offset) continue;
        if (!fn({off, ts, std::move(k), std::move(v)})) break;
    }
}

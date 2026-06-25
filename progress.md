# distq — Build Progress

## Stages

### Stage 1: Partition log (append-only segment + index)
**Status:** Complete
- [x] `src/segment.h/.cpp` — Append-only .log + .index files; sparse offset index for O(log n) seek
- [x] `src/partition.h/.cpp` — Multi-segment partition, segment rolling at 64MB, Fetch with scan

### Stage 2: Broker
**Status:** Complete
- [x] `src/broker.h/.cpp` — Topic/partition registry; FNV-1a key-based partition routing

### Stage 3: Consumer API
**Status:** Complete
- [x] `src/consumer.h/.cpp` — Offset-tracked consumer; Poll + Commit; restart from committed offset
- [x] `CMakeLists.txt`

### Stage 4: Tests + push
**Status:** Complete
- [x] `tests/test_distq.cpp` — 14 tests: Segment, Partition, Broker, Consumer, concurrent producers
- [x] GitHub push

---

## Log

- **2026-06-25** — Repository created. README pushed. Build started.
- **2026-06-25** — All 4 stages complete. Full source pushed to GitHub.

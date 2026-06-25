#include <gtest/gtest.h>
#include "segment.h"
#include "partition.h"
#include "broker.h"
#include "consumer.h"
#include <filesystem>
#include <string>
#include <thread>

namespace fs = std::filesystem;

// ---- Segment tests -----------------------------------------------------

class SegmentTest : public ::testing::Test {
protected:
    std::string dir_ = "test_seg_tmp";
    void SetUp()    override { fs::create_directories(dir_); }
    void TearDown() override { fs::remove_all(dir_); }
};

TEST_F(SegmentTest, AppendAndReadAt) {
    Segment seg(dir_, 0);
    uint64_t off0 = seg.Append("k0", "v0");
    uint64_t off1 = seg.Append("k1", "v1");
    uint64_t off2 = seg.Append("k2", "v2");

    EXPECT_EQ(off0, 0u);
    EXPECT_EQ(off1, 1u);
    EXPECT_EQ(off2, 2u);

    Record r;
    ASSERT_TRUE(seg.ReadAt(0, r)); EXPECT_EQ(r.key, "k0"); EXPECT_EQ(r.value, "v0");
    ASSERT_TRUE(seg.ReadAt(1, r)); EXPECT_EQ(r.key, "k1"); EXPECT_EQ(r.value, "v1");
    ASSERT_TRUE(seg.ReadAt(2, r)); EXPECT_EQ(r.key, "k2"); EXPECT_EQ(r.value, "v2");
}

TEST_F(SegmentTest, ReadAtOutOfRange) {
    Segment seg(dir_, 10); // base offset = 10
    seg.Append("k", "v");
    Record r;
    EXPECT_FALSE(seg.ReadAt(9,  r)); // before base
    EXPECT_FALSE(seg.ReadAt(11, r)); // after last written
}

TEST_F(SegmentTest, Scan) {
    Segment seg(dir_, 0);
    for (int i = 0; i < 5; ++i) seg.Append("k" + std::to_string(i), "v");

    std::vector<uint64_t> offsets;
    seg.Scan(2, [&](const Record& r) -> bool {
        offsets.push_back(r.offset);
        return true;
    });
    ASSERT_EQ(offsets.size(), 3u);
    EXPECT_EQ(offsets[0], 2u);
    EXPECT_EQ(offsets[2], 4u);
}

TEST_F(SegmentTest, ScanEarlyStop) {
    Segment seg(dir_, 0);
    for (int i = 0; i < 10; ++i) seg.Append("k", "v");

    int count = 0;
    seg.Scan(0, [&](const Record&) -> bool { return ++count < 3; });
    EXPECT_EQ(count, 3);
}

// ---- Partition tests ---------------------------------------------------

class PartitionTest : public ::testing::Test {
protected:
    std::string dir_ = "test_part_tmp";
    void SetUp()    override { fs::create_directories(dir_); }
    void TearDown() override { fs::remove_all(dir_); }
};

TEST_F(PartitionTest, AppendAndFetch) {
    Partition p(dir_, "events", 0);
    for (int i = 0; i < 10; ++i)
        p.Append("key" + std::to_string(i), "val" + std::to_string(i));

    auto records = p.Fetch(0, 5);
    ASSERT_EQ(records.size(), 5u);
    EXPECT_EQ(records[0].offset, 0u);
    EXPECT_EQ(records[4].offset, 4u);
}

TEST_F(PartitionTest, FetchFromMiddle) {
    Partition p(dir_, "events", 0);
    for (int i = 0; i < 20; ++i) p.Append("k", "v");

    auto records = p.Fetch(10, 5);
    ASSERT_EQ(records.size(), 5u);
    EXPECT_EQ(records[0].offset, 10u);
}

TEST_F(PartitionTest, NextOffsetAdvances) {
    Partition p(dir_, "events", 0);
    EXPECT_EQ(p.NextOffset(), 0u);
    p.Append("k", "v");
    EXPECT_EQ(p.NextOffset(), 1u);
    p.Append("k", "v");
    EXPECT_EQ(p.NextOffset(), 2u);
}

// ---- Broker tests ------------------------------------------------------

class BrokerTest : public ::testing::Test {
protected:
    std::string dir_ = "test_broker_tmp";
    void TearDown() override { fs::remove_all(dir_); }
};

TEST_F(BrokerTest, ProduceAndFetch) {
    Broker b(dir_);
    b.CreateTopic("orders", {1, 1});

    b.Produce("orders", "key1", "payload1");
    b.Produce("orders", "key1", "payload2");

    auto records = b.Fetch("orders", 0, 0, 10);
    ASSERT_EQ(records.size(), 2u);
    EXPECT_EQ(records[0].value, "payload1");
    EXPECT_EQ(records[1].value, "payload2");
}

TEST_F(BrokerTest, MultiPartitionRouting) {
    Broker b(dir_);
    b.CreateTopic("events", {4, 1});

    // Same key always goes to same partition
    b.Produce("events", "user:42", "a");
    b.Produce("events", "user:42", "b");
    b.Produce("events", "user:99", "x");

    // Fetch all partitions — user:42 messages should be in one partition
    int total = 0;
    for (int p = 0; p < 4; ++p)
        total += static_cast<int>(b.Fetch("events", p, 0, 100).size());
    EXPECT_EQ(total, 3);
}

TEST_F(BrokerTest, UnknownTopicThrows) {
    Broker b(dir_);
    EXPECT_THROW(b.Fetch("nonexistent", 0, 0, 10), std::runtime_error);
}

// ---- Consumer tests ----------------------------------------------------

class ConsumerTest : public ::testing::Test {
protected:
    std::string dir_ = "test_consumer_tmp";
    void TearDown() override { fs::remove_all(dir_); }
};

TEST_F(ConsumerTest, PollReadsInOrder) {
    Broker b(dir_);
    b.CreateTopic("logs", {1, 1});
    for (int i = 0; i < 10; ++i)
        b.Produce("logs", "k", "msg" + std::to_string(i));

    Consumer c(&b, "logs", 0, 0);
    auto batch = c.Poll(10);
    ASSERT_EQ(batch.size(), 10u);
    for (int i = 0; i < 10; ++i)
        EXPECT_EQ(batch[i].value, "msg" + std::to_string(i));
}

TEST_F(ConsumerTest, OffsetAdvancesAfterPoll) {
    Broker b(dir_);
    b.CreateTopic("logs", {1, 1});
    b.Produce("logs", "k", "a");
    b.Produce("logs", "k", "b");

    Consumer c(&b, "logs", 0, 0);
    c.Poll(1);
    EXPECT_EQ(c.CurrentOffset(), 1u);
    c.Poll(1);
    EXPECT_EQ(c.CurrentOffset(), 2u);
}

TEST_F(ConsumerTest, CommitAndResume) {
    Broker b(dir_);
    b.CreateTopic("logs", {1, 1});
    for (int i = 0; i < 5; ++i) b.Produce("logs", "k", std::to_string(i));

    Consumer c(&b, "logs", 0, 0);
    c.Poll(3);
    c.Commit();
    EXPECT_EQ(c.CommittedOffset(), 3u);

    // Simulate restart: new consumer picks up from committed offset
    Consumer c2(&b, "logs", 0, c.CommittedOffset());
    auto rest = c2.Poll(10);
    ASSERT_EQ(rest.size(), 2u);
    EXPECT_EQ(rest[0].value, "3");
    EXPECT_EQ(rest[1].value, "4");
}

TEST_F(ConsumerTest, ConcurrentProducersConsumer) {
    Broker b(dir_);
    b.CreateTopic("stream", {1, 1});

    // Two producers, one consumer — verify all messages arrive in order
    std::thread p1([&] {
        for (int i = 0; i < 50; ++i) b.Produce("stream", "p1", std::to_string(i));
    });
    std::thread p2([&] {
        for (int i = 0; i < 50; ++i) b.Produce("stream", "p1", std::to_string(i));
    });
    p1.join(); p2.join();

    Consumer c(&b, "stream", 0, 0);
    auto all = c.Poll(200);
    EXPECT_EQ(all.size(), 100u);
    // Verify offsets are monotonically increasing
    for (size_t i = 1; i < all.size(); ++i)
        EXPECT_GT(all[i].offset, all[i-1].offset);
}

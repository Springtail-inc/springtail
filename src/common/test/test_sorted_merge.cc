#include <gtest/gtest.h>
#include <common/sorted_merge.hh>
#include <vector>
#include <algorithm>
#include <string>

using namespace springtail;

TEST(SortedMergeTest, EmptyContainers) {
    std::vector<std::vector<int>> containers;
    containers.push_back({});
    containers.push_back({});

    auto merger = common::make_sorted_merge(containers);

    ASSERT_TRUE(merger.empty());
    ASSERT_EQ(merger.size(), 0);
    ASSERT_EQ(merger.begin(), merger.end());
}

TEST(SortedMergeTest, SingleContainer) {
    std::vector<std::vector<int>> containers;
    containers.push_back({1, 3, 5, 7, 9});

    auto merger = common::make_sorted_merge(containers);

    ASSERT_FALSE(merger.empty());
    ASSERT_EQ(merger.size(), 5);

    std::vector<int> result;
    for (auto it = merger.begin(); it != merger.end(); ++it) {
        result.push_back(*it);
    }

    ASSERT_EQ(result.size(), 5);
    ASSERT_EQ(result[0], 1);
    ASSERT_EQ(result[1], 3);
    ASSERT_EQ(result[2], 5);
    ASSERT_EQ(result[3], 7);
    ASSERT_EQ(result[4], 9);
}

TEST(SortedMergeTest, MultipleSortedContainers) {
    std::vector<std::vector<int>> containers;
    containers.push_back({1, 4, 7, 10});
    containers.push_back({2, 5, 8});
    containers.push_back({3, 6, 9, 11});

    auto merger = common::make_sorted_merge(containers);

    ASSERT_FALSE(merger.empty());
    ASSERT_EQ(merger.size(), 11);

    std::vector<int> result;
    for (auto val : merger) {
        result.push_back(val);
    }

    ASSERT_EQ(result.size(), 11);
    for (size_t i = 0; i < result.size(); ++i) {
        ASSERT_EQ(result[i], static_cast<int>(i + 1));
    }
}

TEST(SortedMergeTest, DuplicateValues) {
    std::vector<std::vector<int>> containers;
    containers.push_back({1, 3, 5, 7});
    containers.push_back({1, 3, 5, 7});
    containers.push_back({2, 4, 6, 8});

    auto merger = common::make_sorted_merge(containers);

    ASSERT_EQ(merger.size(), 12);

    std::vector<int> result;
    for (auto val : merger) {
        result.push_back(val);
    }

    std::vector<int> expected = {1, 1, 2, 3, 3, 4, 5, 5, 6, 7, 7, 8};
    ASSERT_EQ(result, expected);
}

TEST(SortedMergeTest, ReverseIteration) {
    std::vector<std::vector<int>> containers;
    containers.push_back({1, 4, 7});
    containers.push_back({2, 5, 8});
    containers.push_back({3, 6, 9});

    auto merger = common::make_sorted_merge(containers);

    std::vector<int> result;
    for (auto it = merger.rbegin(); it != merger.rend(); ++it) {
        result.push_back(*it);
    }

    ASSERT_EQ(result.size(), 9);
    for (size_t i = 0; i < result.size(); ++i) {
        ASSERT_EQ(result[i], static_cast<int>(9 - i));
    }
}

TEST(SortedMergeTest, CustomComparator) {
    std::vector<std::vector<int>> containers;
    containers.push_back({10, 7, 4, 1});
    containers.push_back({9, 6, 3});
    containers.push_back({8, 5, 2});

    auto merger = common::make_sorted_merge(containers, std::greater<int>());

    std::vector<int> result;
    for (auto val : merger) {
        result.push_back(val);
    }

    ASSERT_EQ(result.size(), 10);
    for (size_t i = 0; i < result.size(); ++i) {
        ASSERT_EQ(result[i], static_cast<int>(10 - i));
    }
}

TEST(SortedMergeTest, LowerBound) {
    std::vector<std::vector<int>> containers;
    containers.push_back({1, 3, 5, 7, 9});
    containers.push_back({2, 4, 6, 8, 10});
    containers.push_back({1, 5, 9});

    auto merger = common::make_sorted_merge(containers);

    auto it = merger.lower_bound(5);
    ASSERT_NE(it, merger.end());
    ASSERT_EQ(*it, 5);

    std::vector<int> result;
    for (; it != merger.end(); ++it) {
        result.push_back(*it);
    }

    std::vector<int> expected = {5, 5, 6, 7, 8, 9, 9, 10};
    ASSERT_EQ(result, expected);
}

TEST(SortedMergeTest, LowerBoundNotFound) {
    std::vector<std::vector<int>> containers;
    containers.push_back({1, 2, 3});
    containers.push_back({4, 5, 6});

    auto merger = common::make_sorted_merge(containers);

    auto it = merger.lower_bound(10);
    ASSERT_EQ(it, merger.end());
}

TEST(SortedMergeTest, UpperBound) {
    std::vector<std::vector<int>> containers;
    containers.push_back({1, 3, 5, 5, 7, 9});
    containers.push_back({2, 4, 5, 6, 8, 10});

    auto merger = common::make_sorted_merge(containers);

    auto it = merger.upper_bound(5);
    ASSERT_NE(it, merger.end());
    ASSERT_EQ(*it, 6);

    std::vector<int> result;
    for (; it != merger.end(); ++it) {
        result.push_back(*it);
    }

    std::vector<int> expected = {6, 7, 8, 9, 10};
    ASSERT_EQ(result, expected);
}

TEST(SortedMergeTest, UpperBoundNotFound) {
    std::vector<std::vector<int>> containers;
    containers.push_back({1, 2, 3});
    containers.push_back({4, 5, 6});

    auto merger = common::make_sorted_merge(containers);

    auto it = merger.upper_bound(10);
    ASSERT_EQ(it, merger.end());
}

TEST(SortedMergeTest, IteratorEquality) {
    std::vector<std::vector<int>> containers;
    containers.push_back({1, 2, 3});

    auto merger = common::make_sorted_merge(containers);

    auto it1 = merger.begin();
    auto it2 = merger.begin();

    ASSERT_EQ(it1, it2);
    ASSERT_FALSE(it1 != it2);

    ++it1;
    ASSERT_NE(it1, it2);
    ASSERT_FALSE(it1 == it2);
}

TEST(SortedMergeTest, IteratorPostIncrement) {
    std::vector<std::vector<int>> containers;
    containers.push_back({1, 2, 3});

    auto merger = common::make_sorted_merge(containers);

    auto it = merger.begin();
    ASSERT_EQ(*it, 1);

    auto old_it = it++;
    ASSERT_EQ(*old_it, 1);
    ASSERT_EQ(*it, 2);
}

TEST(SortedMergeTest, StringContainer) {
    std::vector<std::vector<std::string>> containers;
    containers.push_back({"apple", "cherry", "grape"});
    containers.push_back({"banana", "date", "fig"});

    auto merger = common::make_sorted_merge(containers);

    std::vector<std::string> result;
    for (const auto& val : merger) {
        result.push_back(val);
    }

    std::vector<std::string> expected = {"apple", "banana", "cherry", "date", "fig", "grape"};
    ASSERT_EQ(result, expected);
}

TEST(SortedMergeTest, MoveConstructor) {
    std::vector<std::vector<int>> containers;
    containers.push_back({1, 3, 5});
    containers.push_back({2, 4, 6});

    auto merger = common::make_sorted_merge(std::move(containers));

    std::vector<int> result;
    for (auto val : merger) {
        result.push_back(val);
    }

    ASSERT_EQ(result.size(), 6);
    std::vector<int> expected = {1, 2, 3, 4, 5, 6};
    ASSERT_EQ(result, expected);
}

TEST(SortedMergeTest, ConstIterators) {
    std::vector<std::vector<int>> containers;
    containers.push_back({1, 2, 3});
    containers.push_back({4, 5, 6});

    const auto merger = common::make_sorted_merge(containers);

    std::vector<int> result;
    for (auto it = merger.cbegin(); it != merger.cend(); ++it) {
        result.push_back(*it);
    }

    ASSERT_EQ(result.size(), 6);
    std::vector<int> expected = {1, 2, 3, 4, 5, 6};
    ASSERT_EQ(result, expected);
}

TEST(SortedMergeTest, ConstReverseIterators) {
    std::vector<std::vector<int>> containers;
    containers.push_back({1, 2, 3});
    containers.push_back({4, 5, 6});

    const auto merger = common::make_sorted_merge(containers);

    std::vector<int> result;
    for (auto it = merger.crbegin(); it != merger.crend(); ++it) {
        result.push_back(*it);
    }

    ASSERT_EQ(result.size(), 6);
    std::vector<int> expected = {6, 5, 4, 3, 2, 1};
    ASSERT_EQ(result, expected);
}

TEST(SortedMergeTest, SingleElementContainers) {
    std::vector<std::vector<int>> containers;
    containers.push_back({5});
    containers.push_back({2});
    containers.push_back({8});
    containers.push_back({1});

    auto merger = common::make_sorted_merge(containers);

    std::vector<int> result;
    for (auto val : merger) {
        result.push_back(val);
    }

    std::vector<int> expected = {1, 2, 5, 8};
    ASSERT_EQ(result, expected);
}

TEST(SortedMergeTest, MixedEmptyAndNonEmptyContainers) {
    std::vector<std::vector<int>> containers;
    containers.push_back({});
    containers.push_back({1, 3, 5});
    containers.push_back({});
    containers.push_back({2, 4, 6});
    containers.push_back({});

    auto merger = common::make_sorted_merge(containers);

    ASSERT_FALSE(merger.empty());
    ASSERT_EQ(merger.size(), 6);

    std::vector<int> result;
    for (auto val : merger) {
        result.push_back(val);
    }

    std::vector<int> expected = {1, 2, 3, 4, 5, 6};
    ASSERT_EQ(result, expected);
}

TEST(SortedMergeTest, LargeNumberOfContainers) {
    std::vector<std::vector<int>> containers;
    for (int i = 0; i < 100; ++i) {
        containers.push_back({i, i + 100, i + 200});
    }

    auto merger = common::make_sorted_merge(containers);

    ASSERT_EQ(merger.size(), 300);

    std::vector<int> result;
    for (auto val : merger) {
        result.push_back(val);
    }

    ASSERT_EQ(result.size(), 300);
    ASSERT_TRUE(std::is_sorted(result.begin(), result.end()));
}

TEST(SortedMergeTest, ArrowOperator) {
    struct Point {
        int x;
        int y;
        bool operator<(const Point& other) const {
            return x < other.x || (x == other.x && y < other.y);
        }
    };

    std::vector<std::vector<Point>> containers;
    containers.push_back({{1, 2}, {3, 4}});
    containers.push_back({{2, 3}, {4, 5}});

    auto merger = common::make_sorted_merge(containers);

    auto it = merger.begin();
    ASSERT_EQ(it->x, 1);
    ASSERT_EQ(it->y, 2);

    ++it;
    ASSERT_EQ(it->x, 2);
    ASSERT_EQ(it->y, 3);
}

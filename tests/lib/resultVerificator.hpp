#ifndef RESULT_COMPARATOR_HPP_
#define RESULT_COMPARATOR_HPP_

#include <set>
#include <vector>

#include "networkGenerator.hpp"

#include "../../src/immutable/pageIdAndRank.hpp"

class PageIdAndRankComparable : public PageIdAndRank {
public:
    PageIdAndRankComparable(PageIdAndRank const& pageIdAndRankArg)
        : PageIdAndRank(pageIdAndRankArg)
    {
    }

    PageId getPageId() const
    {
        return this->pageId;
    }

    PageRank getPageRank() const
    {
        return this->pageRank;
    }

    bool operator<(PageIdAndRankComparable const& other) const
    {
        if (this->pageId == other.pageId) {
            ASSERT(this->pageId == other.pageId,
                "Same pageId, different content, this=" << *this << ", other=" << other);
        }
        return this->pageId.id < other.pageId.id;
    }
};

class ResultVerificator {
public:
    static void verifyResults(std::vector<PageIdAndRank> const& v1, std::vector<PageRank> const& v2, NetworkGenerator const& generator)
    {
        std::set<PageIdAndRankComparable> set1;
        for (uint32_t i = 0; i < v1.size(); ++i) {
            set1.insert(PageIdAndRank(v1[i]));
        }

        std::set<PageIdAndRankComparable> set2;
        for (uint32_t i = 0; i < v2.size(); ++i) {
            set2.insert(PageIdAndRank(generator.generatePageFromNumWithGeneratedId(i).getId(), v2[i]));
        }
        verifyResults(set1, set2);
    }

    static void verifyResults(std::set<PageIdAndRankComparable> const& set1, std::set<PageIdAndRankComparable> const& set2)
    {
        ASSERT(set1.size() == set2.size(), "Unexpected sizes: set1=" << set1.size() << ", set2=" << set2.size());

        auto iter1 = set1.begin();
        auto iter2 = set2.begin();

        while (iter1 != set1.end()) {

            ASSERT(iter1->getPageId() == iter2->getPageId(), "PageId mismatch: " << iter1->getPageId() << "!=" << iter2->getPageId());
            ASSERT(
                std::abs(iter1->getPageRank() - iter2->getPageRank()) < 0.001,
                "Invalid result, pageId=" << iter1->getPageId() << ", res1=" << iter1->getPageRank() << ", res2=" << iter2->getPageRank());

            ++iter1;
            ++iter2;
        }
    };
};

#endif // RESULT_COMPARATOR_HPP_

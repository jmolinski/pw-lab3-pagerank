#ifndef PAGE_WITH_PAGE_RANK_H_
#define PAGE_WITH_PAGE_RANK_H_

#include "common.hpp"
#include "page.hpp"

typedef double PageRank;

class PageIdAndRank {
public:
    PageIdAndRank(PageId const& pageIdArg, PageRank const& pageRankArg)
        : pageId(pageIdArg)
        , pageRank(pageRankArg)
    {
    }

    PageIdAndRank(PageId const&& pageIdArg, PageRank const& pageRankArg)
        : pageId(std::move(pageIdArg))
        , pageRank(pageRankArg)
    {
    }

private:
    PageId pageId;
    PageRank pageRank;

    friend std::ostream& operator<<(std::ostream& out, PageIdAndRank const& pageIdAndRank);

    // For test only
    friend class PageIdAndRankComparable;
};

std::ostream& operator<<(std::ostream& out, PageIdAndRank const& pageIdAndRank)
{
    out << "(" << pageIdAndRank.pageId << ", " << pageIdAndRank.pageRank << ")";
    return out;
}

#endif // PAGE_WITH_PAGE_RANK_H_

#ifndef PAGE_ID_HPP_
#define PAGE_ID_HPP_

#include <string>

class PageId {
public:
    PageId(std::string const& idArg)
        : id(idArg)
    {
    }

    bool operator==(PageId const& other) const
    {
        return this->id == other.id;
    }

private:
    std::string id;

    friend std::ostream& operator<<(std::ostream& out, PageId const& pageId);

    friend class PageIdHash;
    // For tests only
    friend class PageIdAndRankComparable;
};

class PageIdHash {
public:
    std::size_t operator()(PageId const& pageId) const
    {
        return std::hash<std::string> {}(pageId.id);
    };
};

std::ostream& operator<<(std::ostream& out, PageId const& pageId)
{
    out << pageId.id;
    return out;
}

#endif // PAGE_ID_HPP_

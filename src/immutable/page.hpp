#ifndef SRC_IMMUTABLE_PAGE_HPP_
#define SRC_IMMUTABLE_PAGE_HPP_

#include <functional>
#include <iostream>
#include <optional>
#include <vector>

#include "common.hpp"
#include "idGenerator.hpp"
#include "pageId.hpp"

class Page {
public:
    Page(std::string const& contentArg)
        : id("")
        , isIdComputed(false)
        , content(contentArg)
        , links()
    {
    }

    void generateId(IdGenerator const& idGenerator) const
    {
        ASSERT(not this->isIdComputed, "Generating id twice");
        this->id = idGenerator.generateId(this->content);
        this->isIdComputed = true;
    }

    PageId getId() const
    {
        ASSERT(this->isIdComputed, "Getting id while empty");
        return this->id;
    }

    void addLink(PageId const& link)
    {
        this->links.push_back(link);
    }

    std::vector<PageId> const& getLinks() const
    {
        return this->links;
    }

private:
    mutable PageId id;
    mutable bool isIdComputed; // No std::optional in C++14

    std::string content;
    std::vector<PageId> links;

    friend std::ostream& operator<<(std::ostream& out, Page const& page);
};

std::ostream& operator<<(std::ostream& out, Page const& page)
{
    out << "(";
    out << (page.isIdComputed ? page.id : std::string("NO_ID"));

    out << ", \"" << page.content << "\"";

    out << ", [";
    printContainer(out, page.links);
    out << "])";

    return out;
}

#endif /* SRC_IMMUTABLE_PAGE_HPP_ */

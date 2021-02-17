#ifndef SRC_IMMUTABLE_NETWORK_HPP_
#define SRC_IMMUTABLE_NETWORK_HPP_

#include <vector>

#include "common.hpp"
#include "page.hpp"

class Network {
public:
    Network(IdGenerator const& idGeneratorArg)
        : pages()
        , idGenerator(idGeneratorArg)
    {
    }

    void addPage(Page const& page)
    {
        this->pages.push_back(page);
    }

    size_t getSize() const
    {
        return this->pages.size();
    }

    std::vector<Page> const& getPages() const
    {
        return this->pages;
    }

    IdGenerator const& getGenerator() const
    {
        return this->idGenerator;
    }

private:
    std::vector<Page> pages;
    IdGenerator const& idGenerator;

    friend std::ostream& operator<<(std::ostream& out, Network const& page);
};

std::ostream& operator<<(std::ostream& out, Network const& network)
{
    return printContainer(out, network.pages);
}

#endif /* SRC_IMMUTABLE_NETWORK_HPP_ */

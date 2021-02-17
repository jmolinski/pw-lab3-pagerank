#ifndef PAGE_RANK_COMPUTER_H_
#define PAGE_RANK_COMPUTER_H_

#include <vector>

#include "network.hpp"
#include "pageIdAndRank.hpp"

class PageRankComputer {
public:
    PageRankComputer() {};

    virtual std::vector<PageIdAndRank> computeForNetwork(Network const&, double alpha, uint32_t iterations, double tolerance) const = 0;

    virtual std::string getName() const = 0;

    virtual ~PageRankComputer() { }
};

#endif // PAGE_RANK_COMPUTER_H_

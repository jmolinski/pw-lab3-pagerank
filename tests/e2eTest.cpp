#include <algorithm>
#include <memory>

#include "lib/networkGenerator.hpp"
#include "lib/performanceTimer.hpp"
#include "lib/resultVerificator.hpp"

#include "../src/immutable/common.hpp"
#include "../src/immutable/pageRankComputer.hpp"

#include "../src/multiThreadedPageRankComputer.hpp"
#include "../src/sha256IdGenerator.hpp"
#include "../src/singleThreadedPageRankComputer.hpp"

void pageRankComputationWithNetwork(PageRankComputer const& computer, Network const& network)
{
    PerformanceTimer timer;
    std::vector<PageIdAndRank> result = computer.computeForNetwork(network, 0.85, 100, 0.0000001);
    timer.printTimeDifference("E2E Test [" + computer.getName() + "]");

    ASSERT(result.size() == network.getSize(), "Invalid result size=" << result.size() << ", for network" << network);

    bool found = false;
    for (uint32_t i = 0; i < result.size(); ++i) {
        PageIdAndRankComparable comparable(result[i]);
        if (comparable.getPageId() == PageId("43e255cca92dafcf2d317e795c006ab3ebfcf833fc3a9a9c82cc3cb0a1b5251f")) {
            ASSERT(std::abs(comparable.getPageRank() - 0.000602328499790035) < 0.00001, "Invalid result: " << comparable.getPageRank());
            found = true;
        }
    }
    ASSERT(found, "Test result not found");
}

int main(int argc, char** argv)
{
    ASSERT(argc <= 2, "Too many arguments: " << argc);

    // Prepare computer
    std::shared_ptr<PageRankComputer> computerPtr;
    if (argc == 1) {
        computerPtr = std::shared_ptr<PageRankComputer>(new SingleThreadedPageRankComputer {});
    } else {
        uint32_t numThreads;
        std::stringstream(argv[1]) >> numThreads;
        computerPtr = std::shared_ptr<PageRankComputer>(new MultiThreadedPageRankComputer { numThreads });
    }

    Sha256IdGenerator idGenerator;
    StdinGenerator networkGenerator(idGenerator);
    pageRankComputationWithNetwork(*computerPtr, networkGenerator.generateNetworkOfSize(1200));

    return 0;
}

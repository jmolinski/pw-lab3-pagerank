#include "../src/immutable/common.hpp"
#include "../src/immutable/pageIdAndRank.hpp"

#include "../src/multiThreadedPageRankComputer.hpp"
#include "../src/singleThreadedPageRankComputer.hpp"

#include "./lib/networkGenerator.hpp"
#include "./lib/performanceTimer.hpp"
#include "./lib/resultVerificator.hpp"
#include "./lib/simpleIdGenerator.hpp"

void pageRankComputationWithNumNodes(uint32_t num, PageRankComputer const& computer, NetworkGenerator const& networkGenerator)
{
    Network network = networkGenerator.generateNetworkOfSize(num);
    PerformanceTimer timer;
    std::vector<PageIdAndRank> result = computer.computeForNetwork(network, 0.85, 100, 0.0000001);
    timer.printTimeDifference("PageRank Performance Test [" + std::to_string(num) + " nodes, " + computer.getName() + "]");

    ASSERT(result.size() == network.getSize(), "Invalid result size=" << result.size());
}

int main()
{
    SingleThreadedPageRankComputer computer;
    SimpleIdGenerator simpleIdGenerator("2000f1ffa5ce95d0f1e1893598e6aeeb2c214c85a88e3569d62c2dccd06a8725");
    SimpleNetworkGenerator simpleNetworkGenerator(simpleIdGenerator);

    pageRankComputationWithNumNodes(100, computer, simpleNetworkGenerator);
    pageRankComputationWithNumNodes(1000, computer, simpleNetworkGenerator);
    pageRankComputationWithNumNodes(2000, computer, simpleNetworkGenerator);

    pageRankComputationWithNumNodes(2000, MultiThreadedPageRankComputer { 1 }, simpleNetworkGenerator);
    pageRankComputationWithNumNodes(2000, MultiThreadedPageRankComputer { 2 }, simpleNetworkGenerator);
    pageRankComputationWithNumNodes(2000, MultiThreadedPageRankComputer { 3 }, simpleNetworkGenerator);
    pageRankComputationWithNumNodes(2000, MultiThreadedPageRankComputer { 4 }, simpleNetworkGenerator);
    pageRankComputationWithNumNodes(2000, MultiThreadedPageRankComputer { 8 }, simpleNetworkGenerator);

    NetworkWithoutManyEdgesGenerator networkWithoutEdgesGenerator(simpleIdGenerator);
    pageRankComputationWithNumNodes(500000, computer, networkWithoutEdgesGenerator);
    pageRankComputationWithNumNodes(500000, MultiThreadedPageRankComputer { 1 }, networkWithoutEdgesGenerator);
    pageRankComputationWithNumNodes(500000, MultiThreadedPageRankComputer { 2 }, networkWithoutEdgesGenerator);
    pageRankComputationWithNumNodes(500000, MultiThreadedPageRankComputer { 3 }, networkWithoutEdgesGenerator);
    pageRankComputationWithNumNodes(500000, MultiThreadedPageRankComputer { 4 }, networkWithoutEdgesGenerator);
    pageRankComputationWithNumNodes(500000, MultiThreadedPageRankComputer { 8 }, networkWithoutEdgesGenerator);
    return 0;
}

#ifndef SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_
#define SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_

#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "immutable/network.hpp"
#include "immutable/pageIdAndRank.hpp"
#include "immutable/pageRankComputer.hpp"

class MultiThreadedPageRankComputer : public PageRankComputer {
    using page_hashmap_t = std::unordered_map<PageId, PageRank, PageIdHash>;
    using page_set_t = std::unordered_set<PageId, PageIdHash>;

    struct IterationResult {
        std::vector<PageIdAndRank> result;
        double difference;

        IterationResult(std::vector<PageIdAndRank> &&res, double diff) : result(res), difference(diff) {
        }
    };

  public:
    MultiThreadedPageRankComputer(uint32_t numThreadsArg) : numThreads(numThreadsArg){};

    void generateIdsForPages(Network const &network) const {
        const auto &generator = network.getGenerator();
        std::vector<std::thread> threads;

        for (unsigned threadNum = 0; threadNum < this->numThreads; threadNum++) {
            threads.emplace_back(
                [&](unsigned initial) {
                    for (unsigned i = initial; i < network.getSize(); i += this->numThreads) {
                        network.getPages()[i].generateId(generator);
                    }
                },
                threadNum);
        }

        for (auto &thread : threads) {
            thread.join();
        }
    }

    double runIterationCalc(double initial_value, Network const &network, page_hashmap_t &pageHashMap,
                            page_hashmap_t &previousPageHashMap, double alpha,
                            std::unordered_map<PageId, std::vector<PageId>, PageIdHash> &edges,
                            std::unordered_map<PageId, uint32_t, PageIdHash> &numLinks) const {
        std::vector<std::thread> threads;
        std::vector<PageId> pageIds;
        for (const auto &iter : pageHashMap) {
            pageIds.push_back(iter.first);
        }

        double difference = 0;
        std::mutex diff_mutex;

        for (unsigned threadNum = 0; threadNum < this->numThreads; threadNum++) {
            threads.emplace_back(
                [&](unsigned initial) {
                    double diff = 0;
                    for (unsigned i = initial; i < network.getSize(); i += this->numThreads) {
                        auto pageId = pageIds[i];
                        double v = initial_value;

                        if (edges.count(pageId) > 0) {
                            for (auto link : edges[pageId]) {
                                v += alpha * previousPageHashMap[link] / numLinks[link];
                            }
                        }

                        pageHashMap[pageId] = v;
                        diff += std::abs(previousPageHashMap[pageId] - v);
                    }

                    std::unique_lock<std::mutex> lock(diff_mutex);
                    difference += diff;
                },
                threadNum);
        }

        for (auto &thread : threads) {
            thread.join();
        }

        return difference;
    }

    std::vector<PageIdAndRank> computeForNetwork(Network const &network, double alpha, uint32_t iterations,
                                                 double tolerance) const {
        page_hashmap_t pageHashMap;

        generateIdsForPages(network);
        double initial_page_v = 1.0 / network.getSize();
        for (auto const &page : network.getPages()) {
            pageHashMap[page.getId()] = initial_page_v;
        }

        std::unordered_map<PageId, uint32_t, PageIdHash> numLinks;
        for (auto page : network.getPages()) {
            numLinks[page.getId()] = page.getLinks().size();
        }

        page_set_t danglingNodes;
        for (auto page : network.getPages()) {
            if (page.getLinks().size() == 0) {
                danglingNodes.insert(page.getId());
            }
        }

        std::unordered_map<PageId, std::vector<PageId>, PageIdHash> edges;
        for (auto page : network.getPages()) {
            for (auto link : page.getLinks()) {
                edges[link].push_back(page.getId());
            }
        }

        for (uint32_t i = 0; i < iterations; ++i) {
            IterationResult res = iteration(pageHashMap, danglingNodes, network, alpha, edges, numLinks);

            ASSERT(res.result.size() == network.getSize(),
                   "Invalid result size=" << res.result.size() << ", for network" << network);

            if (res.difference < tolerance) {
                return res.result;
            }
        }

        ASSERT(false, "Not able to find result in iterations=" << iterations);
    }

    IterationResult iteration(page_hashmap_t &pageHashMap, page_set_t const &danglingNodes,
                              Network const &network, double alpha,
                              std::unordered_map<PageId, std::vector<PageId>, PageIdHash> &edges,
                              std::unordered_map<PageId, uint32_t, PageIdHash> &numLinks) const {
        page_hashmap_t previousPageHashMap = pageHashMap;

        double dangleSum = 0;
        for (auto danglingNode : danglingNodes) {
            dangleSum += previousPageHashMap[danglingNode];
        }
        dangleSum = dangleSum * alpha;
        double danglingWeight = 1.0 / network.getSize();
        double initial_value = dangleSum * danglingWeight + (1.0 - alpha) / network.getSize();

        double difference = runIterationCalc(initial_value, network, pageHashMap, previousPageHashMap, alpha,
                                             edges, numLinks);

        std::vector<PageIdAndRank> result;
        for (const auto &iter : pageHashMap) {
            result.push_back(PageIdAndRank(iter.first, iter.second));
        }

        return IterationResult(std::move(result), difference);
    }

    std::string getName() const {
        return "MultiThreadedPageRankComputer[" + std::to_string(this->numThreads) + "]";
    }

  private:
    uint32_t numThreads;
};

#endif /* SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_ */

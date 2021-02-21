#ifndef SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_
#define SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_

#include <mutex>
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

    template <typename T>
    T advanceWrapper(T it, T end, unsigned n) const {
        for (unsigned i = 0; i < n; i++) {
            if (it != end) {
                it = std::next(it, 1);
            }
        }
        return it;
    }

    struct IterationResult {
        std::vector<PageIdAndRank> result;
        double difference;

        IterationResult(std::vector<PageIdAndRank> &&res, double diff) : result(res), difference(diff) {
        }
    };

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

    double runIterationCalc(std::vector<PageIdAndRank> &result, double initial_value, Network const &network,
                            page_hashmap_t &pageHashMap, page_hashmap_t &previousPageHashMap, double alpha,
                            std::unordered_map<PageId, std::vector<PageId>, PageIdHash> &edges,
                            std::unordered_map<PageId, uint32_t, PageIdHash> &numLinks) const {
        std::vector<std::thread> threads;

        double difference = 0;
        std::mutex diff_mutex;

        for (unsigned threadNum = 0; threadNum < this->numThreads; threadNum++) {
            auto it = advanceWrapper(pageHashMap.begin(), pageHashMap.end(), threadNum);

            auto edges_end = edges.end();
            threads.emplace_back(
                [&](unsigned initial, typename page_hashmap_t::iterator iter) {
                    double diff = 0;

                    for (unsigned i = initial; i < network.getSize(); i += this->numThreads) {
                        auto pageId = iter->first;
                        if (iter == pageHashMap.end()) {
                            break;
                        }

                        double v = initial_value;

                        auto edges_page = edges.find(pageId);
                        if (edges_page != edges_end) {
                            for (const auto& link : edges_page->second) {
                                v += alpha * previousPageHashMap[link] / numLinks[link];
                            }
                        }

                        pageHashMap[pageId] = v;
                        diff += std::abs(previousPageHashMap[pageId] - v);
                        result[i] = PageIdAndRank(pageId, v);
                        iter = advanceWrapper(iter, pageHashMap.end(), this->numThreads);
                    }

                    std::unique_lock<std::mutex> lock(diff_mutex);
                    difference += diff;
                },
                threadNum, it);
        }

        for (auto &thread : threads) {
            thread.join();
        }

        return difference;
    }

    double calculateDanglingSum(page_set_t const &danglingNodes, page_hashmap_t &previousPageHashMap) const {
        std::vector<std::thread> threads;

        double total_sum = 0;
        std::mutex diff_mutex;

        for (unsigned threadNum = 0; threadNum < this->numThreads; threadNum++) {
            auto it = advanceWrapper(danglingNodes.begin(), danglingNodes.end(), threadNum);
            threads.emplace_back(
                [&](typename page_set_t::const_iterator iter) {
                    double sum = 0;
                    do {
                        if (iter == danglingNodes.end()) {
                            break;
                        }
                        sum += previousPageHashMap[*iter];
                        iter = advanceWrapper(iter, danglingNodes.end(), this->numThreads);
                    } while (iter != danglingNodes.end());

                    std::unique_lock<std::mutex> lock(diff_mutex);
                    total_sum += sum;
                },
                it);
        }

        for (auto &thread : threads) {
            thread.join();
        }

        return total_sum;
    }

  public:
    MultiThreadedPageRankComputer(uint32_t numThreadsArg) : numThreads(numThreadsArg){};

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

        page_hashmap_t previousPageHashMap = pageHashMap;
        for (uint32_t i = 0; i < iterations; ++i) {
            IterationResult res(std::vector<PageIdAndRank>(), 0);
            if (i % 2 == 0) {
                res = iteration(previousPageHashMap, pageHashMap, danglingNodes, network, alpha, edges,
                                numLinks);
            } else {
                res = iteration(pageHashMap, previousPageHashMap, danglingNodes, network, alpha, edges,
                                numLinks);
            }

            ASSERT(res.result.size() == network.getSize(),
                   "Invalid result size=" << res.result.size() << ", for network" << network);

            if (res.difference < tolerance) {
                return res.result;
            }
        }

        ASSERT(false, "Not able to find result in iterations=" << iterations);
    }

    IterationResult iteration(page_hashmap_t &previousPageHashMap, page_hashmap_t &pageHashMap,
                              page_set_t const &danglingNodes, Network const &network, double alpha,
                              std::unordered_map<PageId, std::vector<PageId>, PageIdHash> &edges,
                              std::unordered_map<PageId, uint32_t, PageIdHash> &numLinks) const {
        double dangleSum = calculateDanglingSum(danglingNodes, previousPageHashMap);
        dangleSum = dangleSum * alpha;
        double danglingWeight = 1.0 / network.getSize();
        double initial_value = dangleSum * danglingWeight + (1.0 - alpha) / network.getSize();

        std::vector<PageIdAndRank> result;
        result.resize(pageHashMap.size(), PageIdAndRank(PageId(""), 0));
        double difference = runIterationCalc(result, initial_value, network, pageHashMap, previousPageHashMap,
                                             alpha, edges, numLinks);

        return IterationResult(std::move(result), difference);
    }

    std::string getName() const {
        return "MultiThreadedPageRankComputer[" + std::to_string(this->numThreads) + "]";
    }

  private:
    uint32_t numThreads;
};

#endif /* SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_ */

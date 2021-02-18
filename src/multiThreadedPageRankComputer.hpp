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

class ThreadPool {
  public:
    ThreadPool(size_t threads) : stop(false) {
        for (size_t i = 0; i < threads; ++i)
            workers.emplace_back([this] {
                for (;;) {
                    std::function<void()> task;

                    {
                        std::unique_lock<std::mutex> lock(this->queue_mutex);
                        this->condition.wait(lock, [this] { return this->stop || !this->tasks.empty(); });
                        if (this->stop && this->tasks.empty())
                            return;
                        task = std::move(this->tasks.front());
                        this->tasks.pop();
                    }

                    task();
                }
            });
    }

    template <class F>
    auto enqueue2(F &&f) -> std::future<typename std::result_of<F()>::type> {
        using return_type = typename std::result_of<F()>::type;

        auto task = std::make_shared<std::packaged_task<return_type()>>(std::bind(std::forward<F>(f)));

        std::future<return_type> res = task->get_future();
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            tasks.emplace([task]() { (*task)(); });
        }
        condition.notify_one();
        return res;
    }

    ~ThreadPool() {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            stop = true;
        }
        condition.notify_all();
        for (std::thread &worker : workers) {
            worker.join();
        }
    }

  private:
    // need to keep track of threads so we can join them
    std::vector<std::thread> workers;
    // the task queue
    std::queue<std::function<void()>> tasks;

    // synchronization
    std::mutex queue_mutex;
    std::condition_variable condition;
    bool stop;
};

/*
class ThreadPoolQueue {
    // tasks = std::deque;
  public:
    // the constructor just launches some amount of workers
    template <class F>
    ThreadPoolQueue(size_t threads, std::vector<std::vector<F>> thread_tasks, std::function<void()> work) {
        for (size_t i = 0; i < threads; ++i)
            workers.emplace_back([&] {
                for (size_t j = 0; j < thread_tasks[i].size(); j++) {
                    work()
                    task = std::move(this->tasks.front());
                    this->tasks.pop();
                    task();
                }
            });
    }

    // add new work item to the pool
    template <class F, class... Args>
    auto enqueue(F &&f, Args &&...args) -> std::future<typename std::result_of<F(Args...)>::type> {
        using return_type = typename std::result_of<F(Args...)>::type;

        auto task = std::make_shared<std::packaged_task<return_type()>>(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...));

        std::future<return_type> res = task->get_future();
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            tasks.emplace([task]() { (*task)(); });
        }
        condition.notify_one();
        return res;
    }

    // add new work item to the pool
    template <class F>
    auto enqueue2(F &&f) -> std::future<typename std::result_of<F()>::type> {
        using return_type = typename std::result_of<F()>::type;

        auto task = std::make_shared<std::packaged_task<return_type()>>(std::bind(std::forward<F>(f)));

        std::future<return_type> res = task->get_future();
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            tasks.emplace([task]() { (*task)(); });
        }
        condition.notify_one();
        return res;
    }

    // the destructor joins all threads
    ~ThreadPoolQueue() {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            stop = true;
        }
        condition.notify_all();
        for (std::thread &worker : workers) {
            worker.join();
        }
    }

  private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
};*/

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

    void generateIdsForPages(unsigned threadsNum, Network const &network) const {
        const auto &generator = network.getGenerator();
        std::vector<std::thread> threads;

        for (unsigned threadNum = 0; threadNum < threadsNum; threadNum++) {
            threads.emplace_back([&](unsigned initial) {
                for (unsigned i = initial; i < network.getSize(); i += threadsNum) {
                    network.getPages()[i].generateId(generator);
                }
            }, threadNum);
        }

        for (auto &thread : threads) {
            thread.join();
        }
    }

    std::vector<PageIdAndRank> computeForNetwork(Network const &network, double alpha, uint32_t iterations,
                                                 double tolerance) const {
        page_hashmap_t pageHashMap;

        generateIdsForPages(this->numThreads, network);
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
            IterationResult res =
                iteration(pageHashMap, danglingNodes, network.getSize(), alpha, edges, numLinks);

            ASSERT(res.result.size() == network.getSize(),
                   "Invalid result size=" << res.result.size() << ", for network" << network);

            if (res.difference < tolerance) {
                return res.result;
            }
        }

        ASSERT(false, "Not able to find result in iterations=" << iterations);
    }

    IterationResult iteration(page_hashmap_t &pageHashMap, page_set_t const &danglingNodes,
                              unsigned networkSize, double alpha,
                              std::unordered_map<PageId, std::vector<PageId>, PageIdHash> &edges,
                              std::unordered_map<PageId, uint32_t, PageIdHash> &numLinks) const {
        page_hashmap_t previousPageHashMap = pageHashMap;

        double dangleSum = 0;
        for (auto danglingNode : danglingNodes) {
            dangleSum += previousPageHashMap[danglingNode];
        }
        dangleSum = dangleSum * alpha;
        double danglingWeight = 1.0 / networkSize;
        double initial_value = dangleSum * danglingWeight + (1.0 - alpha) / networkSize;

        double difference = 0;

        std::mutex diff_mutex;
        {
            ThreadPool tpool(1); // this->numThreads
            for (const auto &pageMapElem : pageHashMap) {
                tpool.enqueue2([&] {
                    double diff_for_page =
                        run_iteration_for_page(initial_value, pageMapElem.first, previousPageHashMap,
                                               pageHashMap, alpha, edges, numLinks);

                    {
                        std::unique_lock<std::mutex> lock(diff_mutex);
                        difference += diff_for_page;
                    }
                });
            }
        }

        std::vector<PageIdAndRank> result;
        for (const auto &iter : pageHashMap) {
            result.push_back(PageIdAndRank(iter.first, iter.second));
        }

        return IterationResult(std::move(result), difference);
    }

    double run_iteration_for_page(double initial_value, typename page_hashmap_t::key_type pageId,
                                  page_hashmap_t &previousPageHashMap, page_hashmap_t &pageHashMap,
                                  double alpha,
                                  std::unordered_map<PageId, std::vector<PageId>, PageIdHash> &edges,
                                  std::unordered_map<PageId, uint32_t, PageIdHash> &numLinks) const {
        auto &pageMapElem = *pageHashMap.find(pageId);
        pageMapElem.second = initial_value;

        for (const auto &link : edges[pageId]) {
            pageMapElem.second += alpha * previousPageHashMap[link] / numLinks[link];
        }

        return std::abs(previousPageHashMap[pageId] - pageMapElem.second);
    }

    std::string getName() const {
        return "MultiThreadedPageRankComputer[" + std::to_string(this->numThreads) + "]";
    }

  private:
    uint32_t numThreads;
};

#endif /* SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_ */

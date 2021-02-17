#ifndef PERFORMANCE_TIMER_H_
#define PERFORMANCE_TIMER_H_

#include <chrono>
#include <iomanip>
#include <iostream>

class PerformanceTimer {
public:
    PerformanceTimer()
    {
        this->startTime = std::chrono::system_clock::now();
    }

    void printTimeDifference(std::string const& activityName)
    {
        auto end = std::chrono::system_clock::now();
        std::chrono::duration<double> diff = end - this->startTime;

        std::cout << activityName << " took: " << std::setw(9) << diff.count() << "s" << std::endl;
    }

private:
    std::chrono::time_point<std::chrono::system_clock> startTime;
};

#endif // PERFORMANCE_TIMER_H_

#ifndef SRC_IMMUTABLE_COMMON_HPP_
#define SRC_IMMUTABLE_COMMON_HPP_

#include <iostream>
#include <sstream>

// do {} while (0) is a trick for mutli-statement macro in C/C++
#define ASSERT(condition, message)               \
    do {                                         \
        if (not(condition)) {                    \
            std::ostringstream str;              \
            str << message;                      \
            std::cerr << str.str() << std::endl; \
            std::abort();                        \
        }                                        \
    } while (0)

template <typename T>
std::ostream& printContainer(std::ostream& out, T const& container)
{
    for (auto iter = container.begin(); iter != container.end(); ++iter) {
        out << *iter;
        if (std::next(iter) != container.end()) // not last
        {
            out << ", ";
        }
    }
    return out;
}

#endif // SRC_IMMUTABLE_COMMON_HPP_

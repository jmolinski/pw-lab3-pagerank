#ifndef TESTS_LIB_SIMPLEIDGENERATOR_HPP_
#define TESTS_LIB_SIMPLEIDGENERATOR_HPP_

#include "../../src/immutable/idGenerator.hpp"

class SimpleIdGenerator : public IdGenerator {
public:
    SimpleIdGenerator(std::string const& hashArg)
        : hash(hashArg)
    {
    }

    virtual PageId generateId(std::string const& content) const
    {
        return this->hash + content;
    }

private:
    std::string hash;
};

#endif /* TESTS_LIB_SIMPLEIDGENERATOR_HPP_ */

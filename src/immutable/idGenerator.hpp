#ifndef SRC_IMMUTABLE_IDGENERATOR_HPP_
#define SRC_IMMUTABLE_IDGENERATOR_HPP_

#include <string>

#include "pageId.hpp"

class IdGenerator {
public:
    virtual PageId generateId(std::string const& content) const = 0;

    virtual ~IdGenerator() {};
};

#endif /* SRC_IMMUTABLE_IDGENERATOR_HPP_ */

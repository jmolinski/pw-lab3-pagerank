#ifndef SRC_SHA256IDGENERATOR_HPP_
#define SRC_SHA256IDGENERATOR_HPP_

#include "immutable/idGenerator.hpp"
#include "immutable/pageId.hpp"

class Sha256IdGenerator : public IdGenerator {
public:
    virtual PageId generateId(std::string const& /*content*/) const
    {
        return PageId("");
    }
};

#endif /* SRC_SHA256IDGENERATOR_HPP_ */

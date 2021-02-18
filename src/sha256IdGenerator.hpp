#ifndef SRC_SHA256IDGENERATOR_HPP_
#define SRC_SHA256IDGENERATOR_HPP_

#include "immutable/idGenerator.hpp"
#include "immutable/pageId.hpp"

#include <sstream>
#include <stdlib.h>

class Sha256IdGenerator : public IdGenerator {
  public:
    virtual PageId generateId(std::string const &content) const {
        std::stringstream stream;
        stream << "echo -n \"" << content << "\" | sha256sum";

        char hash[65];

        FILE *fp = popen(stream.str().c_str(), "r");
        if (fp == NULL || (fread(hash, 64, 1, fp) != 1)) {
            exit(EXIT_FAILURE);
        }
        pclose(fp);

        hash[64] = 0;
        return PageId(std::string(hash));
    }
};

#endif /* SRC_SHA256IDGENERATOR_HPP_ */

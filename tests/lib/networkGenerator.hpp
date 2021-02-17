#ifndef NETWORK_GENERATOR
#define NETWORK_GENERATOR

#include "../../src/immutable/network.hpp"

class NetworkGenerator {
public:
    NetworkGenerator(IdGenerator const& idGeneratorArg)
        : idGenerator(idGeneratorArg)
    {
    }

    virtual Network generateNetworkOfSize(uint32_t const) const = 0;
    virtual ~NetworkGenerator() {};

    Page generatePageFromNum(uint32_t num) const
    {
        return Page(std::to_string(num));
    }

    Page generatePageFromNumWithGeneratedId(uint32_t num) const
    {
        Page page = this->generatePageFromNum(num);
        page.generateId(this->idGenerator);
        return page;
    }

protected:
    IdGenerator const& idGenerator;
};

class SimpleNetworkGenerator : public NetworkGenerator {
public:
    SimpleNetworkGenerator(IdGenerator const& idGeneratorArg)
        : NetworkGenerator(idGeneratorArg)
    {
    }

    virtual Network generateNetworkOfSize(uint32_t const size) const
    {
        Network network(this->idGenerator);
        for (uint32_t i = 0; i < size; ++i) {
            Page page = this->generatePageFromNum(i);

            for (uint32_t j = 0; j < size; ++j) {
                if (i == j) {
                    continue;
                }

                if ((((i + 1) * 1337) ^ 0xc0ffee) % size > ((j + 2) * (j + 7)) % size) {
                    page.addLink(this->generatePageFromNumWithGeneratedId(j).getId());
                }
            }

            network.addPage(page);
        }

        return network;
    }
};

class NetworkWithoutManyEdgesGenerator : public NetworkGenerator {
public:
    NetworkWithoutManyEdgesGenerator(IdGenerator const& idGeneratorArg)
        : NetworkGenerator(idGeneratorArg)
    {
    }

    Network generateNetworkOfSize(uint32_t const size) const
    {
        Network network(this->idGenerator);
        uint32_t connectedPartSize = size / 1000;

        for (uint32_t i = 0; i < connectedPartSize; ++i) {
            Page page = this->generatePageFromNum(i);

            for (uint32_t j = 0; j < i; j++) {
                if (i == j) {
                    continue;
                }
                if ((i + j) % 7 == 3) {
                    page.addLink(this->generatePageFromNumWithGeneratedId(j).getId());
                }
            }
            network.addPage(page);
        }

        for (uint32_t i = connectedPartSize; i < size; ++i) {
            Page page = this->generatePageFromNum(i);

            if (i % 1000 == 333) {
                page.addLink(this->generatePageFromNumWithGeneratedId(i - 127).getId());
            }
            network.addPage(page);
        }

        return network;
    }
};

class StdinGenerator : public NetworkGenerator {
public:
    StdinGenerator(IdGenerator const& idGeneratorArg)
        : NetworkGenerator(idGeneratorArg)
    {
    }

    Network generateNetworkOfSize(uint32_t const size) const
    {
        Network network(this->idGenerator);

        std::string numberOfNodesStr;
        std::getline(std::cin, numberOfNodesStr);
        uint32_t numberOfNodes = std::stoul(numberOfNodesStr);
        ASSERT(numberOfNodes == size, "Incorrect size=" << size << ", fromStdin=" << numberOfNodes);

        for (uint32_t i = 0; i < numberOfNodes; ++i) {
            std::string content;
            std::getline(std::cin, content);
            Page page(content);

            std::string edges;
            std::getline(std::cin, edges);

            std::stringstream edgesStream(edges);
            std::string edge;
            while (edgesStream >> edge) {
                page.addLink(PageId(edge));
            }
            //page.generateId(generator);
            network.addPage(page);
        }

        return network;
    }
};

#endif // NETWORK_GENERATOR


#include "../src/immutable/common.hpp"

#include "../src/sha256IdGenerator.hpp"

void testSha256(std::string const& testScenario, std::string const& expectedResult)
{
    Sha256IdGenerator generator;
    PageId result = generator.generateId(testScenario);
    ASSERT(result == PageId(expectedResult),
        "Incorrect SHA256, scenario=" << testScenario
                                      << ", result=" << result
                                      << ", expectedResult=" << expectedResult);
}

int main()
{
    testSha256("Ala ma kota\n", "c51bc001db0206126e1681ba88497ce583f077a92e427e4f62da96b691d28813");
    testSha256("Zawartość naprawdę naprawdę naprawdę wielkiego pliku 1234567890\n", "69bddbdc52992ae9952d3368d48bfe0517ce346d6040e495de3542926294498b");

    return 0;
}

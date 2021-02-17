#!/bin/bash
set -e

echo ">>>>>>>>> Run clang-format"
FILES=$(find | grep "hpp$\|cpp$" | grep -v CMake)
clang-format -i -style=WebKit $FILES

#clang-tidy $FILES # not available on students?

echo ">>>>>>>>> Run tests in DEBUG"
# Run tests in debug config
cmake -DCMAKE_BUILD_TYPE=Debug .
make

/usr/bin/valgrind valgrind --error-exitcode=123 --leak-check=full ./tests/pageRankCalculationTest

./tests/sha256Test
./tests/pageRankCalculationTest
./tests/pageRankPerformanceTest

./tests/e2eTest < ./tests/e2eScenario.txt
for i in 1 2 7; do ./tests/e2eTest $i < ./tests/e2eScenario.txt; done

echo ">>>>>>>>> Run tests in RELEASE"
# Run tests in release config
cmake -DCMAKE_BUILD_TYPE=Release .
make

./tests/sha256Test
./tests/pageRankCalculationTest
./tests/pageRankPerformanceTest

./tests/e2eTest < ./tests/e2eScenario.txt
for i in 1 2 3 4 8; do ./tests/e2eTest $i < ./tests/e2eScenario.txt; done

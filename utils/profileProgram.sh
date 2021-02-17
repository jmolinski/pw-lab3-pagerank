#!/bin/bash
# Profiler based on http://poormansprofiler.org/

programToProfile=$1
outputName=$2

sleeptime=0.1

# Sample stack traces:


echo "" > $outputName #make file empty
echo "Started profiling"

# Support input redirection, example:
# ./utils/profileProgram.sh "./tests/e2eTest 4" output.gdb tests/e2eScenario.txt
if [ "$#" -ne 3 ]; then
	$programToProfile & 
else
	echo $3
	$programToProfile < $3 &
fi

pid=$!
while kill -0 $pid 2> /dev/null; do
	gdb -ex "set pagination 0" -ex "thread apply all bt" -batch -p $pid >> $outputName 2> /dev/null
	sleep $sleeptime
done

echo "Stopped profiling"
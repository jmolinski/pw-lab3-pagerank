git clone https://github.com/brendangregg/FlameGraph > /dev/null 2> /dev/null || true

input=$1
output=$2
cat $1 | ./FlameGraph/stackcollapse-gdb.pl | ./FlameGraph/flamegraph.pl > $2

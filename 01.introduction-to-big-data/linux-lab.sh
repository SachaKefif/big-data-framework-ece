#!/usr/bin/env bash
# Run in Linux, WSL, or the Onyxia terminal. All changes stay in a temporary directory.
set -euo pipefail
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
cd "$work"

mkdir -p data/bronze data/silver logs
printf 'id,value\n1,10\n2,20\n' > data/bronze/sample.csv
printf 'INFO started\nERROR invalid row\nINFO finished\n' > logs/pipeline.log
test "$(head -n 2 data/bronze/sample.csv | tail -n 1)" = '1,10'
test "$(grep -c ERROR logs/pipeline.log)" = 1
cp data/bronze/sample.csv data/silver/sample.csv
mv data/silver/sample.csv data/silver/validated.csv
test -f data/silver/validated.csv
printf 'Directory listing:\n'
ls -R
printf '\nLog search with line numbers:\n'
grep -n -C 1 ERROR logs/pipeline.log
printf '\nLinux lab checks passed.\n'

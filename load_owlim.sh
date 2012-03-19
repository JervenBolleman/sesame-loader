#!/bin/bash
set -o errexit # -e does not work in Shebang-line!
set -o pipefail
set -o nounset

usage="USAGE: $0 [input file] [database directory] [commit interval] [threads] [baseUri]"
: ${5? $usage}

mvn clean install

sh sesame-loader-runtime/dist/bin/load-owlim -infile $1 -dataFile $2 -commitInterval $3 -pushThreads $4 -baseUri $5

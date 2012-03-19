#!/bin/bash
set -o errexit # -e does not work in Shebang-line!
set -o pipefail
set -o nounset

usage="USAGE: $0 [input file] [database directory] [commit interval] [threads] [baseUri]"
: ${5? $usage}


mvn install
cp="";
for i in `find ~/.m2/ -name *.jar`;do
    cp="$cp:$i";
done;

java -classpath "sesame-loader-main/target/sesame-loader-main-0.1.0-SNAPSHOT.jar$cp:conf/" loader -infile $1 -dataFile $2 -databaseProvider native -commitInterval $3 -pushThreads $4 -baseUri $5

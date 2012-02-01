#!/bin/bash
set -o errexit # -e does not work in Shebang-line!
set -o pipefail
set -o nounset

usage="USAGE: $0 [input file] [database directory] [owlim/native] [commit interval] [threads] [baseUri] [owlim.jar] [crypto.jar]"
: ${8? $usage}

cp="";
for i in `find ~/.m2/ -name *.jar`;do
    if [[ ! "$i" =~ "owlim-shim.*" ]]; then
        cp="$cp:$i";
    else
        echo "Excluding $i"
    fi
done;

cp="$cp:$7:$8"
java -classpath "target/sesame-loader-0.1.0-SNAPSHOT.jar$cp:conf/" loader -infile $1 -dataFile $2 -databaseProvider $3 -commitInterval $4 -pushThreads $5 -baseUri $6

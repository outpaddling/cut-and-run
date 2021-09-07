#!/bin/sh -e

make clean all
generand fasta 10 50 > test.fa
./cut-and-run test.fa cat input- .fa

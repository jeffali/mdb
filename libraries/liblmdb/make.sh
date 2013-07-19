#!/bin/bash
GCC="gcc -std=c99 -pthread -O2 -g -W -Wall -Wno-unused-parameter -Wbad-function-cast  -fPIC"

$GCC -c mdb.c
$GCC -c midl.c
ar rs liblmdb.a mdb.o midl.o
gcc -pthread -shared  -o liblmdb.so mdb.o midl.o 

#sequential running of DB threads
$GCC -c paratest.c
$GCC  paratest.o liblmdb.a  -o paratest

#sequential running of DB threads
$GCC -c paratest2.c
$GCC  paratest2.o liblmdb.a  -o paratest2

#parallel running of two processes (1 and 2)
$GCC -c paratest3.c
$GCC  paratest3.o liblmdb.a  -o paratest3


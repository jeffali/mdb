#!/bin/bash
GCC="gcc -pthread -O2 -g -W -Wall -Wno-unused-parameter -Wbad-function-cast  -fPIC"

$GCC -c mdb.c
$GCC -c midl.c
ar rs liblmdb.a mdb.o midl.o
gcc -pthread -shared  -o liblmdb.so mdb.o midl.o 
$GCC -c paratest.c
$GCC  paratest.o liblmdb.a  -o paratest



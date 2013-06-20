#!/bin/bash
gcc -g -c mdb.c
gcc -g -c mtest2.c
gcc -g -c mtest3.c
gcc -o mtest2 mdb.o mtest2.o -lssl
gcc -o mtest3 mdb.o mtest3.o -lssl

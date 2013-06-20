#!/bin/bash
gcc -std=c99 -c tchmttest.c -I$HOME/dev/other/tokyocabinet-1.4.48
gcc -std=c99 -o tchmttest tchmttest.o -lpthread -ltokyocabinet


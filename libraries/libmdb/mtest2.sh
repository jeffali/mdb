#!/bin/bash
for x in /tmp/testdb.* ; do n=`echo $x|cut -d. -f2`; od -v -Ax -tx -w32 $x > /tmp/DIFF.$n ;done

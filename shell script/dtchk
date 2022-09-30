#!/bin/bash

name=$(cat /etc/hosts | grep -E "^192.168" | tr -s ' ' | cut -d ' ' -f2)

for i in $name
do
  nc -w 1 -z $i 22
  [ "$?" == 0 ] && echo "$i connected"
  [ "$?" != 0 ] && echo "$i connection failed"
done

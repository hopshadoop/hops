#!/bin/bash
rm stat
grep  "$1.*TX Stats" $2 > stat
sed -i 's/.*Total Time: //g' stat
sed -i 's/ms//g' stat
awk '{ total += $1; count++ } END { print total/count }' stat 




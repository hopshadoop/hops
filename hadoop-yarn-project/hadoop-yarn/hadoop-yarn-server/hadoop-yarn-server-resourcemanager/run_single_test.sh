#!/bin/bash

TestName=$1
ResultFile=$2
FAILED_DIR=$3
 echo "**** Running Test ($TestName) ****" >>  /tmp/hop_tx_stats.txt 
 echo "mvn -e -X -Dtest=$TestName test"
       mvn -e -X -Dtest=$TestName test
 OUT=$?	
 
 if [ $OUT -eq 0 ]; then 
   echo "$TestName PASSED" >> $ResultFile
 else
   echo "$TestName FAILED" >> $ResultFile
 fi

 TestNameSimple=`echo $TestName | sed 's/.java//g'` 
  FullPath=`find . -iname "*$TestNameSimple-output.txt"` 
  if [ -n "$FullPath" ]; then
    mv $FullPath ./$FAILED_DIR/
    echo "mv $FullPath ./$FAILED_DIR/"
  fi






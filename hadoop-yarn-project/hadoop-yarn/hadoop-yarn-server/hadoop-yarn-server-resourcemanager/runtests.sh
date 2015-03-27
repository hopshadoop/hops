#!/bin/bash

TestNames=$1
MaxTime=1200
TestResultsParent=results
AllTestsResults=$TestResultsParent/AllTestsResults.txt
FAILED_DIR=$TestResultsParent/all_tests_reports

rm -rf $TestResultsParent
mkdir -p $FAILED_DIR
PID=$$
mkdir -p /tmp/$PID
for i in $(cat $TestNames); do
 UnitTestResult="/tmp/$PID/$i.HOP_Results"
 rm $UnitTestResult

 ./run_single_test.sh $i  $UnitTestResult $FAILED_DIR &

 TimePassed=1;
 while [ $TimePassed -le $MaxTime ];
 do
  sleep 1
  TimePassed=`expr $TimePassed + 1`
   if [ -f $UnitTestResult ]; then
      break
   fi
  #echo $TimePassed
 done
 
 killall java
 sleep 5
 find . -iname "test.log" -exec rm -f {} \;	

 if [ -f $UnitTestResult ]; then
   cat $UnitTestResult >> $AllTestsResults
 fi
 	
 if [ $TimePassed -ge $MaxTime ]; then
    sed -i "s/$i FAILED/$i TIMEOUT FAILED/g" $AllTestsResults
    #echo "    sed -i \"s/$i/$i TIMEOUT/g\" $AllTestsResults"
 fi 
	
done


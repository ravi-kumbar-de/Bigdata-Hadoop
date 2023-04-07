#!/bin/bash

TABLESLIST=$1
#10.0.0.153
COORDINATOR_IP=$2
COUNT_OUTPUT_FILE=$3
IFS=$'\n'


#='2019-06-02'

for TAB in `cat $TABLESLIST`
do

beforecount=$(impala-shell -i $COORDINATOR_IP -B --quiet -q "select count(*) from $TAB;")
altertable=$(impala-shell -i $COORDINATOR_IP -B --quiet -q "ALTER TABLE $TAB RECOVER PARTITIONS;")
aftercount=$(impala-shell -i $COORDINATOR_IP -B --quiet -q "select count(*) from $TAB;")
#echo "$var"
echo "$TAB|$beforecount|$aftercount" >> count/$3

done
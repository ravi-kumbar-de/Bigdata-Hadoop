#!/bin/sh
file_arr=()
echo "==================shell script start========================"
echo -en '\n'
echo "Checking Directory : " $1
echo -en '\n'
echo "Content before deletion :"
echo "===================================="
hadoop fs -ls $1 | awk '{print $8}'
echo "===================================="
beforesize=$(hadoop fs -count $1| awk '{print $3}')
beforesize=$(bc <<< "scale = 3; $beforesize / 1000000")

for file in $(hadoop fs -ls $1 | grep -v $2 | awk '{print $8}')
do
    file_arr+=("$file")
done
#hadoop fs -rm -r "${file_arr[@]}" 
echo -en '\n'
echo "Files Deleted :"
echo "===================================="
( IFS=$'\n'; echo "${file_arr[*]}" )
echo "===================================="
echo -en '\n'
echo "Remaining file :"
echo "===================================="
hadoop fs -ls $1 | awk '{print $8}'
aftersize=$(hadoop fs -count $1 | awk '{print $3}')
aftersize=$(bc <<< "scale = 3; $aftersize / 1000000")
echo "===================================="
echo "BEFORE DELETION SIZE : " $beforesize "MB"
echo "AFTER DELETION SIZE : " $aftersize "MB"
echo -en '\n'
echo "================== shell script end ========================"
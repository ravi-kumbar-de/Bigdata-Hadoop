#!/bin/sh

for file in /home/rkumbar/wcsrestoredddl/*.sql; do
        if [ -s $file ]
        then
                hive -f $file >> /home/rkumbar/wcs/wcs.log
        fi
done;
wait

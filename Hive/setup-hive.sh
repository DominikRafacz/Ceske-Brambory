#!/bin/bash
FILE=/home/brambory/hive/setup.hql
if [ -f "$FILE" ]; then
    echo "$FILE found. Setting up hive table"
    hive -S -f $FILE
else 
    echo "$FILE does not exist."
fi

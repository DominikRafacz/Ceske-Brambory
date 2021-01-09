#!/bin/bash
if [[ $(echo "exists 'events'" | hbase shell | grep 'not exist') ]];
then
    echo "create 'events','h', 'r'" | hbase shell;
fi
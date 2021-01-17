#!/bin/bash
if [[ $(echo "exists 'events'" | hbase shell | grep 'not exist') ]];
then
    echo "create 'events','h', 'r', 'c'" | hbase shell;
fi

/usr/hdp/current/hbase-master/bin/hbase-daemon.sh stop rest
/usr/hdp/current/hbase-master/bin/hbase-daemon.sh start rest -p 8000


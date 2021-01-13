#!/bin/bash

su nifi
mkdir /home/nifi/detector_data
mkdir /home/nifi/hits_data
exit

su brambory
hadoop  fs -mkdir /user/brambory
exit


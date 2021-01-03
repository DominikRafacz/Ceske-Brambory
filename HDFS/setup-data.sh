#!/bin/bash

su nifi
mkdir /home/nifi/detector_data
exit

su brambory
hadoop  fs -mkdir /user/brambory
exit
#!/bin/bash

su nifi
cd /home/nifi
mkdir detector_metadata

exit

su brambory
hadoop  fs -mkdir /user/brambrory

exit

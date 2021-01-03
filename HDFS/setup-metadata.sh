#!/bin/bash

su nifi
mkdir /home/nifi/detector_metadata

exit

su brambory
hadoop  fs -mkdir /user/brambory

exit

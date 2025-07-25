#!/bin/bash

# Exit on error
set -e

# Run the python script according to our configuration
cd ~/workspace/bi/jobs/ness/


# bash ~/workspace/bi/jobs/ness/scheduler/execute_my_kaggle_daily.sh

python3 my_kaggle.py bi-course-461012 --etl-name etl --etl-action daily

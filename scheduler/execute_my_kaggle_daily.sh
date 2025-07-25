#!/bin/bash

# Exit on error
set -e

# Run the python script according to our configuration
cd ~/workspace/bi/


# bash ~/workspace/bi/jobs/ness/scheduler/execute_my_kaggle_daily.sh

python3 jobs/ness/my_kaggle.py bi-course-461012 --etl-name etl --etl-action daily

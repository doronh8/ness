#!/usr/bin/env python3
# -*- coding: utf-8 -*-
""""
repo_name = bi
repo_tail = jobs/ness/
home = /Users/doron/workspace


python3 bi/jobs/ness/my_kaggle.py bi-course-461012 --etl-name etl --etl-action daily


/Users/doron/.config/kaggle/kaggle.json

ls /home/doron_h8j
nano /home/doron_h8j/.config/kaggle/kaggle.json

{"username":"doronhadad","key":"6065b66acd456ea4b051263004ed6552"}

pip3 install pandas-gbq --break-system-packages

chmod 600 /home/doron_h8j/.config/kaggle/kaggle.json
"""

print("start")

from pathlib import Path
import os
import requests
import json
import re
import sys
from google.cloud import bigquery
import argparse
from datetime import datetime, timedelta, date
import uuid
import platform
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi


# adapt the env to mac or windows
if os.name == 'nt':
    home = Path("C:/workspace/")
else:
    home = Path(os.path.expanduser("~/workspace/"))


# get repository name
repo_name = re.search(r"(.*)[/\\]workspace[/\\]([^/\\]+)", __file__).group(2)
repo_tail = re.search(r".*[/\\]"+repo_name+r"[/\\](.+)[/\\]", __file__).group(1)
sys.path.insert(0, str(home / f"{repo_name}/utilities/"))

# print(f"repo name is: {repo_name}  \t repo tail is {repo_tail}")

from my_etl_files import readJsonFile, ensureDirectory, writeFile, readFile

def process_command_line(argv):
    if argv is None:
        argv = sys.argv[1:]
    # initialize the parser object:

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("project_id",
                        help="""Operation to perform. The arguments for each option are:
                        Full_Load:   --date""",
                        default="bi-course-461012")
    parser.add_argument("--etl-action", choices=["init", "daily", "delete","step"], help="""The action the etl job""")
    parser.add_argument("--etl-name", help="""The name of the etl job""")
    parser.add_argument("--dry-run", help="""if True don't execute the queries""", action="store_true")
    parser.add_argument("--days-back", help="""The number of days we want to go back""",
                        default=1)

    return parser, argparse.Namespace()


parser, flags = process_command_line(sys.argv[1:])
x = sys.argv[1:]
parser.parse_args(x, namespace=flags)

# define the project_id
project_id = flags.project_id
etl_name = flags.etl_name
etl_action = flags.etl_action
days_back = int(flags.days_back)

step_id = 0
env_type = etl_action
log_table = f"{project_id}.logs.daily_logs"


# Construct a BigQuery client object.
client = bigquery.Client(project=project_id)

# Get dates
date_today = date.today()
y_m_d = (date_today + timedelta(days=-days_back)).strftime("%Y-%m-%d")
ymd = y_m_d.replace("-", "")


# init log dict
log_dict = {'ts': datetime.now(),
            'dt': datetime.now().strftime("%Y-%m-%d"),
            'uid': str(uuid.uuid4())[:8],
            'username': platform.node(),
            'job_name': etl_name,
            'job_type': etl_action,
            'file_name': os.path.basename(__file__),
            'step_name': 'start',
            'step_id': step_id,
            'log_type': env_type,
            'message': str(x)
            }


# functions
def set_log(log_dict, step, log_table=log_table):
    log_dict['step_name'] = step
    log_dict['step_id'] += 1
    log_dict['ts'] = datetime.now()
    log_dict['dt'] = datetime.now().strftime("%Y-%m-%d")
    job = client.load_table_from_dataframe(pd.DataFrame(log_dict, index=[0]), log_table)
    job.result() # Wait for the job to complete.

ROW_LIMITS = {
    "init": 100,    #set init if first-time run - notice write disposition is TRUNCATE
    "step": 50,     #set step for testing small batches
    "daily": 900    # daily run, notice write disposition is APPEND
}

if not flags.dry_run:
    set_log(log_dict, "start - imports and functions retrieved.")

# get etl configuration
etl_configuration = readJsonFile(home / repo_name / repo_tail / f"config/{etl_name}_config.json")
# etl_conf = readJsonFile(home / "bi/jobs/kaggle/screen_time_impact_on_mental_health/config/etl_config.json")["screen_time_impact_on_mental_health"]
if not flags.dry_run:
    set_log(log_dict, "Connected to config file")


for etl_name, etl_conf in etl_configuration.items():
        # if etl_name != "fmcg_daily_sales":
        #     continue  # skip all other datasets
        if not etl_conf['isEnable']:
            continue
        print(f"{etl_name}\n{len(etl_name)*'='}\n")

        if not flags.dry_run:
            set_log(log_dict, f"{flags.etl_action.upper()} connected to : {etl_name}")

        # ===== DELETE Logic =====
        if etl_action == "delete":
            table_id = etl_conf['table_id']
            print(f"Deleting BigQuery table: {table_id}")
            client.delete_table(table_id, not_found_ok=True)
            print(f"Table {table_id} deleted.")

            if not flags.dry_run:
                set_log(log_dict, f"Deleted table {table_id}")

            continue

        # ===== INIT / STEP / DAILY =====
        print("Extract")

        if not flags.dry_run:
            set_log(log_dict, f"Extract {etl_name}")

        # Kaggle dataset info
        # "abhishekdave9/digital-habits-vs-mental-health-dataset"

        # Define target path
        target_path = os.path.expanduser(home / f"temp/data/{etl_conf['data_folder']}")
        # print(target_path)

        # Make sure the target directory exists
        # os.makedirs(target_path, exist_ok=True)
        ensureDirectory(target_path)

        # Set up the API
        api = KaggleApi()
        api.authenticate()

        # Dataset identifier from Kaggle
        dataset = f"{etl_conf['producer']}/{etl_conf['dataset_name']}"
        # dataset = "abhishekdave9/digital-habits-vs-mental-health-dataset"

        # Download and unzip the dataset into the target directory and handle non-successful response
        try:
            api.dataset_download_files(dataset, path=target_path, unzip=True)
        except Exception as e:
            error_msg = f"\n \t NOTICE: Failed to download dataset '{etl_name}':{e}"
            print(error_msg)
            if not flags.dry_run:
                log_dict["message"] = error_msg
                set_log(log_dict, f"Error in Kaggle Download {etl_name}")
                log_dict["message"] = str(x)
            continue  # Skip to next ETL job



        # List the downloaded files to verify
        # print("Files in target directory:", os.listdir(target_path))
        print(F"Files in target directory: {target_path}")


        print("Transform")
        if not flags.dry_run:
            set_log(log_dict, f"Transform {etl_name}")

        data_file = f"{target_path}/{etl_conf['file_name']}"


        # Clean column headers: replace spaces with underscores
        df = pd.read_csv(data_file)
        original_columns = df.columns.tolist()
        df.columns = [col.strip().replace(" ", "_") for col in df.columns]
        df.to_csv(data_file, index=False)

        # print("Column headers cleaned:")
        for old, new in zip(original_columns, df.columns):
            if old != new:
                print(f"  '{old}' → '{new}'")

        print("Load")
        if not flags.dry_run:
            set_log(log_dict, f"Load {etl_name}")

        # Slice rows based on etl_action
        limit = ROW_LIMITS.get(etl_action, 20)

        # TODO(developer): Set table_id to the ID of the table to create.
        # table_id = "your-project.your_dataset.your_table_name"
        table_id = etl_conf['table_id']


        # Get previously loaded row count (basic example)
        # def get_last_loaded(table_id):   #first run only
        #     try:
        #         # query = f"SELECT MAX(row_number) as last FROM `{project_id}.logs.etl_tracking` WHERE etl_name = '{etl_name}'"
        #         # query = f"SELECT MAX(row_number) as last FROM `{project_id}.logs.daily_logs` WHERE etl_name = '{etl_name}'"
        #         query = f"SELECT count(*) as last FROM `{etl_conf['table_id']}` WHERE etl_name = '{etl_name}'"
        #         result = list(client.query(query).result())
        #         return result[0].last or 0
        #     except Exception:
        #         return 0
        def get_last_loaded(table_id):
            try:
                table = client.get_table(table_id)
                return table.num_rows
            except Exception as e:
                print(f"Error getting last loaded rows: {e}")
                return 0


        start = get_last_loaded(table_id)
        end = start + limit
        df_batch = df.iloc[start:end]

        #  Skip this dataset if there are no rows left to process
        if df_batch.empty:
            print(f"Skipping {etl_name} — no new rows to load.")
            continue

        # Clean problematic quotes in description column
        if "description" in df_batch.columns:
            def fix_quotes(val):
                if isinstance(val, str) and val.count('"') % 2 != 0:
                    return val.replace('"', "'")
                return val


            df_batch["description"] = df_batch["description"].apply(fix_quotes)

        # Save cleaned batch
        df_batch.to_csv(data_file, index=False)

        print(f"Loading {len(df_batch)} rows from {start} to {end} into {table_id}")

        if not flags.dry_run:
            job_config = bigquery.LoadJobConfig(
                # autodetect=True, #first load only
                autodetect=False, #
                # write_disposition="WRITE_TRUNCATE",
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                skip_leading_rows=1,
                # The source format defaults to CSV, so the line below is optional.
                source_format=bigquery.SourceFormat.CSV,
            )

            with open(data_file, "rb") as source_file:
                job = client.load_table_from_file(source_file, table_id, job_config=job_config)

            job.result()  # Waits for the job to complete.



            table = client.get_table(table_id)  # Make an API request.
            msg = f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}"
            print(msg)
            if not flags.dry_run:
                log_dict['message'] = msg
                set_log(log_dict, f"Load finished {etl_name}")
                log_dict['message'] = str(x)

        if not flags.dry_run:
            set_log(log_dict, "end")

print("\n\n\tDone.")

"""
SELECT rank, count(*) rank_count FROM `bi-course-461012.ness.vg_sales` group by 1 order by rank_count desc, rank desc limit 10
"""
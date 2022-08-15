#!/usr/bin/env python3
#
# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# remember to set your environment variable GOOGLE_APPLICATION_CREDENTIALS to point to a service-account key file. e.g:
# `export GOOGLE_APPLICATION_CREDENTIALS="/path/to/keyfile.json"`

from __future__ import print_function, unicode_literals

import json
import re
import sys
from datetime import datetime
from typing import Union

from InquirerPy import inquirer
from InquirerPy.base import Choice
from google.cloud import bigquery

# A unified datetime-format to be used across the script. Can be changed.
DT_FORMAT = "%Y-%m-%d %H:%M:%S"

# A compiled pattern of valid BigQuery dataset names. Used for validation in case a user chooses to create a new
# dataset.
dataset_name_pattern = re.compile("^[a-zA-Z_]{1,1024}$")

# One BigQuery Client to rule them all
client = bigquery.Client()


def timestamp(dt: datetime) -> int:
    """Utility function to convert datetime objects to a timestamp, milliseconds from epoch.

    Args:
        dt: an instance of datetime.datetime

    Returns:
        Integer representing the number of milliseconds since epoch time
    """
    epoch = datetime.utcfromtimestamp(0)
    return int((dt - epoch).total_seconds() * 1000.0)


def validate_datetime(val: str) -> Union[str, bool]:
    """A validation function to validate a string as a valid datetime string (according to pre-defined format)

    Args:
        val: String. The value to validate

    Returns:
        Either a str or bool. If bool, it will be a True value, signaling validation has passed. A str return
        signals an error. The string itself will contain the error message.
    """
    try:
        datetime.strptime(val, DT_FORMAT)
    except:
        return "There was a problem with the date format"
    else:
        return True


def generate_confirm_message(**kwargs) -> str:
    """A method to generate a multi-line formatted string version of the answers given by the User. Used to get
    confirmation from the user to all his answers.
    Args:
        kwargs: A dictionary with all the user answers

    Returns:
        A formatted, multi-line string, in JSON format.
    """
    _answers = {
        "source_dataset": kwargs["source_dataset"].dataset_id,
        "source_tables":  [t.table_id for t in kwargs["source_tables"]],
        "point_in_time":  kwargs["point_in_time"].strftime(DT_FORMAT),
        "target_dataset": {
            "create_new":
                kwargs["target_dataset"] is None,
            "name":
                kwargs["target_dataset"].dataset_id
                if kwargs["target_dataset"] else kwargs["target_dataset_name"],
        }
    }
    return json.dumps(_answers, indent=2)


def validate_dataset_name(dataset_name: str) -> bool:
    """Function to validate a new BigQuery dataset name.
    Args:
        dataset_name: The suggested name

    Returns:
        Boolean. True if the suggested name is valid. False otherwise.
    """
    match = dataset_name_pattern.match(dataset_name)
    return match is not None


def main():
    # Get all projects
    projects = list(client.list_projects())  # type: list[bigquery.client.Project]
    source_project = inquirer.fuzzy(
        message="Select a source project (up and down keys to move, enter to select)",
        choices=[Choice(value=p, name=p.friendly_name) for p in projects],
        default=None,
        mandatory=True,
        mandatory_message="(Required)",
    ).execute()  # type: bigquery.client.Project

    datasets = {
        d.dataset_id: d
        for d in client.list_datasets(project=source_project.project_id)
    }  # type: dict[str, bigquery.client.Dataset]
    source_dataset = inquirer.select(
        message="Select a source dataset (up and down keys to move, enter to select)",
        choices=[
            Choice(value=dataset, name=dataset.dataset_id)
            for dataset in datasets.values()
        ],
        default=None,
        mandatory=True,
        mandatory_message="(Required)",
    ).execute()  # type: bigquery.client.Dataset

    tables = {t.table_id: t for t in client.list_tables(dataset=source_dataset)}
    source_tables = inquirer.select(
        message="Select a source tables (Space to (de)select, up and down arrows to move):",
        choices=[
            Choice(value=table, name=table.table_id) for table in tables.values()
        ],
        default=None,
        mandatory=True,
        mandatory_message="(Required)",
        multiselect=True,
        validate=lambda res: len(res) > 0,
        invalid_message="Minimum 1 table.",
    ).execute()  # type: list[bigquery.client.Table]

    target_dataset = inquirer.select(
        message="Select a target dataset (Select one or choose create new):",
        choices=[
                    Choice(value=dataset, name=dataset.dataset_id)
                    for dataset in datasets.values()
                ] + [Choice(value=None, name="--- CREATE NEW ---")],
        default=None,
    ).execute()  # type: bigquery.client.Dataset
    target_dataset_name = None
    if not target_dataset:
        target_dataset_name = inquirer.text(
            message="Enter the name for the new Dataset: ",
            mandatory=True,
            validate=validate_dataset_name,
        ).execute()

    point_in_time = inquirer.text(
        message="The Point-in-Time (UTC) to make the clones. Must be in the last 7 days.\n"
                "Specify in the format of 'yyyy-mm-dd HH:MM:ss'\n(4 digits for year, 2 digits for month, day, hours, "
                "minutes and seconds):\n  > ",
        validate=validate_datetime,
        filter=lambda v: datetime.strptime(v, DT_FORMAT),
        default=datetime.utcnow().strftime(DT_FORMAT),
        mandatory=True,
    ).execute()  # type: datetime
    pretty_json = generate_confirm_message(
        source_tables=source_tables,
        source_dataset=source_dataset,
        target_dataset=target_dataset,
        point_in_time=point_in_time,
        target_dataset_name=target_dataset_name)
    confirmed = inquirer.confirm(
        message=f"Review configuration\n{pretty_json}").execute()
    if not confirmed:
        # If the user has not confirmed the selection
        print("Cancel...")
        sys.exit(1)

    if target_dataset is None and target_dataset_name:
        print(f"Creating dataset {target_dataset_name}...")
        target_dataset = bigquery.Dataset(
            f"{source_dataset.project}.{target_dataset_name}")
        target_dataset.location = source_dataset._properties["location"]
        target_dataset = client.create_dataset(target_dataset, timeout=30)
    assert target_dataset is not None and target_dataset.dataset_id is not None

    jobs = []
    project = source_dataset.project
    source_dataset_id = source_dataset.dataset_id
    target_dataset_id = target_dataset.dataset_id
    dt_short = point_in_time.strftime("%Y%m%d%H%M%S")
    ts = timestamp(point_in_time)
    for table in source_tables:
        # for each table, run 2 copy jobs, one is a snapshot, and one a clone.
        # Both tables will be copied using the same timestamp
        source_id = f"{project}.{source_dataset_id}.{table.table_id}@{ts}"
        snapshot_id = f"{project}.{target_dataset_id}.snap_{dt_short}_{table.table_id}"
        clone_id = f"{project}.{target_dataset_id}.clone_{dt_short}_{table.table_id}"

        # the call to `client.copy_table` is asynchronous, which means we can just continue, and wait for all to
        # complete at the end.
        print(
            f"Creating snapshot for {source_dataset_id}.{table.table_id} as {snapshot_id}"
        )
        jobs.append(
            client.copy_table(
                source_id,
                snapshot_id,
                job_config=bigquery.job.CopyJobConfig(operation_type="SNAPSHOT")))
        print(
            f"Creating clone for {source_dataset_id}.{table.table_id} as {clone_id}"
        )
        jobs.append(
            client.copy_table(
                source_id,
                clone_id,
                job_config=bigquery.job.CopyJobConfig(operation_type="CLONE")))
    # wait for all job results
    [job.result() for job in jobs]
    print("All tables created")

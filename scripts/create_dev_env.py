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

import argparse
import dataclasses
import json
import sys
from argparse import ArgumentParser, Namespace
from datetime import datetime
from io import TextIOWrapper
from typing import List, Optional

import google
from google.cloud import bigquery

# A unified datetime-format to be used across the script. Can be changed.
DT_FORMAT = "%Y-%m-%dT%H:%M:%S"

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


def source_tables(astring: str) -> bigquery.Table:
    try:
        tbl = client.get_table(astring)
    except Exception as e:
        print(e)
        raise e
    return tbl


def target_dataset(astring: str) -> bigquery.DatasetReference:
    try:
        ref = bigquery.DatasetReference.from_string(astring, client.project)
    except Exception as e:
        print(e)
        raise e
    return ref


def datetime_type(astring: str) -> datetime:
    return datetime.strptime(astring, DT_FORMAT)


def get_parser() -> ArgumentParser:
    parser = ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                            prog="create-dev-env", description="Create DEV environment for BigQuery Data Integration")
    parser.add_argument("--source-table", required=True, type=source_tables, action='append', dest='source_tables',
                        help="Source table(s). List of tables to be cloned into the development environment. "
                             "Use the BigQuery syntax of dataset and table name seperated by dots, "
                             "with optional project_id as a prefix."
                             "Examples: --source-table my_dataset1.table1 "
                             "--source-table different_project.my_dataset2.table2")
    parser.add_argument("--target-dataset", required=True, type=target_dataset, dest='target_dataset',
                        help="The target dataset to which to clone the source tables. "
                             "If the `--create-dataset` option is specified, dataset MUST NOT be already exists, "
                             "and it will be created. "
                             "If the `--create-dataset` option is NOT specified, the dataset MUST be already exists.")
    parser.add_argument("--create-dataset", action='store_true', dest='create_dataset',
                        help="Changes the behavior of searching for a target dataset. "
                             "If specified, the program will throw an error in case a dataset already exists or create "
                             "it and continue. If not specified, the program will throw an error in case the dataset "
                             "does not exists, or use the dataset for creating the development environment.")
    parser.add_argument("--when", type=datetime_type, default=datetime.utcnow().strftime(DT_FORMAT), dest='when',
                        help=f"The point-in-time to take clones and snapshots of the source tables. "
                             f"Specify in the format 'YYYY-mm-ddTHH:MM:SS'")
    parser.add_argument("--translation-file", help="Create a translation JSON file.", required=False,
                        type=argparse.FileType("w", encoding='UTF-8'), dest="translation_file")

    return parser


@dataclasses.dataclass()
class ProgramArguments:
    source_tables: List[bigquery.Table]
    target_dataset: bigquery.DatasetReference
    create_dataset: bool
    when: datetime
    translation_file: Optional[TextIOWrapper]

    def __init__(self, ns: Namespace):
        self.when = ns.when
        self.source_tables = ns.source_tables
        self.target_dataset = ns.target_dataset
        self.create_dataset = ns.create_dataset
        self.translation_file = ns.translation_file


def main():
    # Get all projects
    parser = get_parser()
    args = parser.parse_args(sys.argv[1:])
    args = ProgramArguments(args)

    if args.create_dataset:
        try:
            target_dataset = client.create_dataset(args.target_dataset, exists_ok=False)
        except google.cloud.exceptions.Conflict:
            parser.error(f"Dataset {args.target_dataset.path} already exists. Either Choose a non-existing dataset "
                         f"name, or remove the `--create-dataset` flag.")
            return
    else:
        try:
            target_dataset = client.get_dataset(args.target_dataset)
        except google.cloud.exceptions.NotFound:
            parser.error(f"Dataset {args.target_dataset.path} does not exists. Either Choose an existing dataset "
                         f"name, or add the `--create-dataset` flag to create it.")
            return

    jobs = []
    dt_short = args.when.strftime("%Y%m%d%H%M%S")
    ts = timestamp(args.when)
    translations = {}
    for table in args.source_tables:
        # for each table, run 2 copy jobs, one is a snapshot, and one a clone.
        # Both tables will be copied using the same timestamp
        source_id = f"{table.project}.{table.dataset_id}.{table.table_id}@{ts}"
        snapshot_id = f"{target_dataset.project}.{target_dataset.dataset_id}.snap_{dt_short}_{table.table_id}"
        clone_id = f"{target_dataset.project}.{target_dataset.dataset_id}.clone_{dt_short}_{table.table_id}"
        translations[f"{table.dataset_id}.{table.table_id}"] = clone_id
        translations[f"{table.project}.{table.dataset_id}.{table.table_id}"] = clone_id
        # the call to `client.copy_table` is asynchronous, which means we can just continue, and wait for all to
        # complete at the end.
        print(
            f"Creating snapshot for {table.project}.{table.dataset_id}.{table.table_id} as {snapshot_id}"
        )
        jobs.append(
            client.copy_table(
                source_id,
                snapshot_id,
                job_config=bigquery.job.CopyJobConfig(operation_type="SNAPSHOT")))
        print(
            f"Creating clone for {table.project}.{table.dataset_id}.{table.table_id} as {clone_id}"
        )
        jobs.append(
            client.copy_table(
                source_id,
                clone_id,
                job_config=bigquery.job.CopyJobConfig(operation_type="CLONE")))
    # wait for all job results
    [job.result() for job in jobs]
    print("All tables created")
    if args.translation_file:
        json.dump(translations, args.translation_file, indent=2)
        args.translation_file.close()
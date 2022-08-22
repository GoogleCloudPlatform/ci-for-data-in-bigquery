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
import json
import os
import sys
from argparse import ArgumentParser
import asyncio
from string import Template
from typing import Optional

import google.auth
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery


class TemplateWithDefaultKey(Template):
    """
    Class extending string.Template to provide a fallback if the key is not present in the translations
    This will be used in the SQL files, where users can simply add the '$' prefix to the all the table(s) in the query.
    If the table is being tested, then it should appear in the translation map, pointing the table to a different
    location.
    If the table is not being tested, it will fall back to the right table.
    """

    # Added a dot (.) to the base pattern, as it is used in query patterns to separate dataset from table names
    idpattern = r'(?a:[_a-z][_a-z0-9\.]*)'

    def __init__(self, template):
        super().__init__(template)

    def substitute(self, *args, **kwds):
        try:
            return super().substitute(*args, **kwds)
        except KeyError as err:
            key = str(err.args[0])
            kwds[key] = key
            return self.substitute(*args, **kwds)


def get_parser() -> ArgumentParser:
    arg_parser = ArgumentParser()
    arg_parser.add_argument('--translation-file', required=False, help='The JSON translation file of tables',
                            dest='translation_file')
    arg_parser.add_argument("TEST_FILE_OR_DIR_PATH", help="The test path. Can be directory of SQL files or a specific "
                                                          "file")
    arg_parser.add_argument("--project", help="The default project to use. "
                                              "Value must be set if not using a service account.",
                            required=False)
    return arg_parser


def read_json_as_dict(translation_file: str) -> dict[str:str]:
    with open(translation_file, 'r') as fp:
        translations = json.load(fp)
    return translations


def create_bigquery_client(project: str) -> bigquery.Client:
    client = bigquery.Client(project=project)
    return client


def get_tests_to_run_from_file(test_file_path: str, translations: dict[str:str]) -> dict[str:str]:
    basename = os.path.splitext(os.path.basename(test_file_path))[0]
    with open(test_file_path) as fp:
        sql_content = " ".join(fp.readlines())
    sql_queries = list(
        map(lambda x: f"{x};",
            filter(lambda x: len(x) > 0,
                   sql_content.strip().split(";"))))
    results = {}
    for i, sql_query_raw in enumerate(sql_queries):
        key_name = f"{basename}_{i}"
        query_template = TemplateWithDefaultKey(sql_query_raw)
        query = query_template.substitute(translations)
        results[key_name] = query
    return results


def get_tests_to_run(test_file_path: str, translations: dict[str:str]) -> dict[str:str]:
    if not os.path.exists(test_file_path):
        raise Exception(f"{test_file_path} does not exists")
    if os.path.isfile(test_file_path):
        test_result = get_tests_to_run_from_file(test_file_path, translations)
        return test_result
    assert os.path.isdir(test_file_path)
    results = {}
    for file in os.listdir(test_file_path):
        results.update(get_tests_to_run_from_file(os.path.join(test_file_path, file), translations))
    return results


def r_pad(s: str, str_len: int, char: str = " ") -> str:
    base_len = len(s)
    spaces = char * max(str_len - base_len, 0)
    return s + spaces


async def result_with_key(query: str, bigquery_client: bigquery.Client):
    try:
        job = bigquery_client.query(query)
        while not job.done():
            await asyncio.sleep(1)
        if job.error_result:
            return job.error_result['message']
        else:
            return "OK"
    except BadRequest as br:
        return br.errors[0]['message']
    except Exception as e:
        return str(e)


async def run(translation_file: str, test_file_path: Optional[str], project: str) -> int:
    """
    Main entry point
    """
    translations = read_json_as_dict(translation_file) if translation_file else {}
    bigquery_client = create_bigquery_client(project)
    tests_to_run = get_tests_to_run(test_file_path, translations)
    max_test_name = max(map(lambda x: len(x), tests_to_run.keys()))
    print(f"{r_pad('Test Name', max_test_name)} | Result")
    print(f"{r_pad('', max_test_name, '-')}-+-------------------------")
    exit_code = 0
    for key_name, query in tests_to_run.items():
        res = await result_with_key(query=query, bigquery_client=bigquery_client)
        if res != "OK":
            exit_code = 2
        print(f"{r_pad(key_name, max_test_name)} | {res}")
    return exit_code


async def main():
    parser = get_parser()
    args = parser.parse_args(sys.argv[1:])
    project = args.project
    if not project:
        _, project = google.auth.default()
        if not project:
            parser.error("Could not infer project from environment. "
                         "You must supply project_id using the `--project` parameter.")
            sys.exit(1)
    exit_code = await run(translation_file=args.translation_file, test_file_path=args.TEST_FILE_OR_DIR_PATH,
                          project=project)
    sys.exit(exit_code)


if __name__ == "__main__":
    asyncio.run(main())

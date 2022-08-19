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
import logging
import os
import sys
from argparse import ArgumentParser
from string import Template
from typing import Optional, Mapping

from google.cloud import bigquery
import google.auth


class TemplateWithDefaultKey(Template):
    """
    Class extending string.Template to provide a fallback if the key is not present in the translations
    This will be used in the SQL files, where users can simply add the '$' prefix to the all the table(s) in the query.
    If the table is being tested, then it should appear in the translation map, pointing the table to a different
    location.
    If the table is not being tested, it will fall back to the right table.
    """

    def __init__(self, template):
        super().__init__(template)

    def substitute(self, *args, **kwds):
        try:
            return super().substitute(*args, **kwds)
        except KeyError as err:
            key = str(err.args[0])
            kwds[key] = key
            return self.substitute(*args, **kwds)

    def safe_substitute(self, __mapping: Mapping[str, object] = ..., **kwds: object) -> str:
        try:
            return super().safe_substitute(__mapping, **kwds)
        except KeyError as err:
            key = str(err.args[0])
            kwds[key] = key
            return self.safe_substitute(__mapping, **kwds)


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


def run_test_file(bigquery_client: bigquery.Client, translations: dict[str:str],
                  test_file_path: str) -> dict:
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
        query = query_template.safe_substitute(translations)
        logging.debug(f"From file {test_file_path} - executing query: '{query}'")
        try:
            result = bigquery_client.query(query, project=bigquery_client.project)
            if result.error_result:
                results[key_name] = f"ERROR {result.error_result}"
        except Exception as e:
            logging.exception("Caught exception during execution, not thrown by BigQuery", exc_info=e)
            results[key_name] = str(e)
        results[key_name] = "OK"
    return results


def run_tests(bigquery_client: bigquery.Client, translations: dict[str:str], test_file_path: str):
    if not os.path.exists(test_file_path):
        raise Exception(f"{test_file_path} does not exists")
    if os.path.isfile(test_file_path):
        test_result = run_test_file(bigquery_client, translations, test_file_path)
        return test_result
    assert os.path.isdir(test_file_path)
    results = {}
    for file in os.listdir(test_file_path):
        results.update(run_test_file(bigquery_client, translations, os.path.join(test_file_path, file)))
    return results


def r_pad(s: str, str_len: int, char: str = " ") -> str:
    base_len = len(s)
    spaces = char * max(str_len - base_len, 0)
    return s + spaces

def run(translation_file: str, test_file_path: Optional[str], project: str) -> int:
    """
    Main entry point
    """
    translations = read_json_as_dict(translation_file) if translation_file else {}
    bigquery_client = create_bigquery_client(project)
    test_results = run_tests(bigquery_client, translations, test_file_path)
    str_len = max(map(lambda x: len(x), test_results.keys()))
    print(f"{r_pad('Test Name', str_len)} | Result")
    print(r_pad("", str_len, "-") + "---------")
    exit_code = 0
    for test_name, result in test_results.items():
        print(f"{r_pad(test_name, str_len)} | {result}")
        if result != "OK":
            exit_code = 2
    return exit_code


def main():
    parser = get_parser()
    args = parser.parse_args(sys.argv[1:])
    project = args.project
    if not project:
        _, project = google.auth.default()
        if not project:
            parser.error("Could not infer project from environment. "
                         "You must supply project_id using the `--project` parameter.")
            sys.exit(1)
    exit_code = run(translation_file=args.translation_file, test_file_path=args.TEST_FILE_OR_DIR_PATH, project=project)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()

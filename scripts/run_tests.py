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
from google.cloud import bigquery


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


def get_parser() -> ArgumentParser:
    arg_parser = ArgumentParser()
    arg_parser.add_argument('--translation-file', required=True, help='The JSON translation file of tables',
                            dest='translation_file')
    arg_parser.add_argument("TEST_FILE_OR_DIR_PATH", help="The test path. Can be directory of SQL files or a specific "
                                                          "file")
    return arg_parser


def read_json_as_dict(translation_file: str) -> dict[str:str]:
    with open(translation_file, 'r') as fp:
        translations = json.load(fp)
    return translations


def create_bigquery_client() -> bigquery.Client:
    client = bigquery.Client()
    return client


def run_test_file(bigquery_client: bigquery.Client, translations: dict[str:str],
                  test_file_path: str) -> dict:
    key_name = os.path.splitext(os.path.basename(test_file_path))[0]
    with open(test_file_path) as fp:
        sql_content = " ".join(fp.readlines())
    query_template = TemplateWithDefaultKey(sql_content)
    query = query_template.substitute(translations)
    logging.debug(f"From file {test_file_path} - executing query: '{query}'")
    try:
        result = bigquery_client.query(query)
        if result.error_result:
            return {key_name: f"ERROR {result.error_result}"}
    except Exception as e:
        logging.exception("Caught exception during execution, not thrown by BigQuery", exc_info=e)
        return {key_name: str(e)}
    return {key_name: "OK"}


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


def run(translation_file: str, test_file_path: str) -> int:
    """
    Main entry point
    """
    translations = read_json_as_dict(translation_file)
    bigquery_client = create_bigquery_client()
    test_results = run_tests(bigquery_client, translations, test_file_path)
    print("Test Name | Result")
    exit_code = 0
    for test_name, result in test_results.items():
        print(f"{test_name} | {result}")
        if result != "OK":
            exit_code = 2
    return exit_code


def main():
    parser = get_parser()
    args = parser.parse_args(sys.argv[1:])
    exit_code = run(translation_file=args.translation_file, test_file_path=args.TEST_FILE_OR_DIR_PATH)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()

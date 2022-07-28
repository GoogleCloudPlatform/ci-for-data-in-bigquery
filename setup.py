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


from setuptools import setup, find_packages

setup(
    name='ci-for-data-in-bigquery',
    version='0.1.0',

    py_modules=find_packages(),
    install_requires=[
        "google-cloud-bigquery==3.2.0",
        "InquirerPy==0.3.4",
    ],
    entry_points={
        'console_scripts': [
            'create-dev-env = scripts.create_dev_env:main',
        ],
    },
)

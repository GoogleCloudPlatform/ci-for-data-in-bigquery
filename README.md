# CI for Data in BigQuery CLI utility

This repo contains 2 CLI utilities that will help to orchestrate CI/CD workflow for data in BigQuery.

The first utility, `create-dev-env` will help orchestrate the process of creating Table Snapshots and Table Clones for the purpose of implementing changes inside the BigQuery Data Warehouse.
This tool automates parts of the process described in the document [CI For Data in BigQuery](https://cloud.google.com/architecture/continuous-integration-of-data-in-bigquery). The tool was written more for demonstration purposes and is probably worth customizing before putting to use in production.

The second utility `run-tests` will run a set of predefined tests, written in SQL to test the data. The sql test statements are written in a way that are meant to fail if the test fails. The queries are loaded and before execution, the target table is replaced (depending on runtime arguments) in order to either test the original table or the test table.
See the section [Writing Tests](#writing-tests) 


## Prerequisites
- Python 3+ (tested using Python 3.8.12 & 3.9.9)
- virtualenv (Recommended)

## Authentication
To run the utility you will need to authenticate using a service account. This can be accomplished either by running inside an authorized VM on GCP, or
setting the environment variable `GOOGLE_APPLICATION_CREDENTIALS` to a valid keyfile. See more
at [Authenticating as a service account](https://cloud.google.com/docs/authentication/production).

### Required Permissions
For the base dataset, where the original tables are residing:
- `bigquery.datasets.get`
- `bigquery.datasets.getData`
- `bigquery.tables.list`

To create a dataset:
- `bigquery.datasets.create`

On the dataset where the table clones and snapshots will reside:
- `bigquery.tables.create`
- `bigquery.tables.createSnapshot`

To run tests against the test tables, on the new dataset:
- `bigquery.tables.getData`
## The process
### A Gif is worth a thousand words
![Example run](./example-run.gif)

## In words
The `create-dev-end` CLI utility will ask for several inputs in order to coordinate a snapshot and clone creation, on which the developer can start implementing their changes in isolation. All clones and snapshots will be a "point-in-time".
On activation, the script will ask for the following:
1. A source project to select from a list of available projects (as assigned to the service account).
2. A source Dataset, from a list of datasets available in the source project.
3. A list of source tables. Multiple selection is possible, using the space key to select or deselect the options. The list of tables is fetched from the selected source dataset.
4. A target dataset. Either an existing one, or select the option to create a new dataset. In which case, the user will be prompted to enter a new dataset name. Validation of the new name is applied.
5. A datetime format, that will be used as a "Point-in-Time" for snapshot and clone creation. This must be a point in time in the last 7 days.
6. A final confirmation of all the details.

Upon confirmation, the script will create the target dataset if required, and create a snapshot and a clone for each table in the list provided. Each snapshot name will be in the form of `snap_<DATETIME>_<SOURCE_TABLE_NAME>` and each clone name will be in the format of `clone_<DATETIME>_<SOURCE_TABLE_NAME>`, where `DATETIME` will be in the format of 4 digits for the year and 2 digits for month, day, hour, minute & second.
e.g: for a source table of the name foo, a snapshot might be named `snap_20220317151941_foo` and the corresponding clone will be named `clone_20220317151941_foo`.

## Writing Tests
In the `sql_tests` directory, you can place multiple sql files. In each file, the sql statement should be designed to throw an error in case the test should fail. See [Debugging Functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/debugging_functions) and [Debugging Statements](https://cloud.google.com/bigquery/docs/reference/standard-sql/debugging-statements) for more information.

In each statement, the target table should be written without a dataset prefix, and with a dollar sign ('$') prefix.
For example, assume the following SQL statement:
```sql
ASSERT (
    (SELECT COUNT(*) 
    FROM dataset1.bar as b 
    LEFT JOIN dataset2.foo as f 
        ON f.id = b.foo_id
    WHERE f.id IS NULL) = 0
) AS 'All bar records must have a valid foo id that corrosponds to a record in the foo table'
 ```
This statement will run in BigQuery, joining `dataset1.bar` as a test table and the foo table as a reference table.
In that case, we would rewrite only the `dataset1.bar` table to be:
```sql
ASSERT (
    (SELECT COUNT(*) 
    FROM $bar as b 
    LEFT JOIN dataset2.foo as f 
        ON f.id = b.foo_id
    WHERE f.id IS NULL) = 0
) AS 'All bar records must have a valid foo id that corrosponds to a record in the foo table'
 ```.
At runtime, the `$bar` would be replaced to the desired test table.

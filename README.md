![](https://raw.githubusercontent.com/dav009/dbtmock/main/dessan_ningyou.png)
# dbtmock
 
`dbtmock` is an utility for end to end testing DBT pipelines. 


`dbtmock` lets you:
- pick a model to test
- mock model dependencies 
- mock a final result
- assert output of a model against a final result dataset

`dbtmock` does not mess with your dbt project, it simply generates SQL queries based on your project `manifests.json`. These SQL queries can be used as regular dbt tests.

## Workflow
  
1. Select a model you want to test
2. Picked any arbitrarly dependencies to Mock. Any intermediate models or sources are Ok. The mocked data is provided via CSV files.
3. Provide expected output result using a CSV file
4. Use `dbtmock` cli to generate test queries
5. Generated queries are run via `dbt test` command


## Installation

### Prebuilt binary
- Check Github releases, Download your needed binary

### Golang
- `go install github.com/dav009/dbtmock/cmd/dbtmock@latest`

## Usage
`dbtmock --manifest target/manifest.json --tests unit_tests --output tests`

- `--manifest` is the path to your dbt's project `manifest.json`
- `--tests` is a path to a folder contaning `json` files. Each json file in that folder is a test definition (see examples below)
- `--output` is the path folder where the generated tests will be stored. By default it points to `tests` folder where `dbt` looks for custom tests to run


## Detailed usage

1. Go to your dbt project: `cd my_project`
2. Create a folder to store your tests definitions: `mkdir unit_tests`
3. Create a json file per test definition. For example the Json file below is a single test in which there are two mocks `seed.jaffle_shop.raw_customers` and  `seed.jaffle_shop.raw_orders`. The test will run the model `"model.jaffle_shop.customers"`. The content of `output` has the data which will be used for assertions.

``` json
{
    "name": "dummy_test",
    "model": "model.jaffle_shop.customers",
    "mocks": {
        "seed.jaffle_shop.raw_customers": {
            "filepath": "seeds/raw_customers.csv",
            "types": {
                "id": "INT64"
            }
        },
        "seed.jaffle_shop.raw_orders": {
            "filepath": "seeds/raw_orders.csv",
            "types": {
                "id": "INT64",
                "user_id": "INT64",
                "order_date":"DATE"
            }
        },   
    },
    "output": {
        "filepath": "output.csv",
        "types": {
            "customer_id": "INT64",
            "most_recent_order": "DATE",
            "number_of_orders": "INT64",
            "customer_lifetime_value": "FLOAT64",
            "first_order": "DATE"
        }
    }
}

```

4. Go to your dbt project, make sure you generated a `manifest.json` (e.g: run `dbt compile`)
5. run `dbtmock --tests unit_tests --output tests`
6. Check the files in the output folder : `ls tests`
7. Run `dbt test` 
8. You should see your tests in the list of test being ran by dbt


### Example project

Take a look at [dbtmock_example](https://github.com/dav009/dbtmock_example)
- This a fork of the vanilla `jaffle_store` project,  which is the `hello world` of dbt
- This fork has a few changes:
 - `unit_tests` folder with a sample `test.json`
 - some mock data as csv: `mocked_data/output.csv` as a dataset to assert the final output of the given test
- it uses already given `csv` files as mocked data

You can use this fork to run `dbtmock`

1. Clone the project `git clone ..`
2. Create a `profile.yml` with your BigQuery settings
3. Run `dbt seed`
4. Run `dbt compile`
5. Run `dbtmock` (no arguments needed)
6. See test generated in : `tests/dummy_test.sql`
7. Run `dbt test` see in the output the result: `START test dummy_test.. PASS dummy_test`


## Manual build

- `go build cmd/dbtmock/dbtmock.go`


## Details

- This project uses `manifest.json` to figure out what are the dependencies of the model that you are testing.
- Given a model to test `dbtmock` recursively rebuilds the sql queries accross dependencies
- While rebuilding a model SQL `dbtmock` replaces table for given mocked data
- `dbtmock` creates a giant SQL query with mocks and model SQL logic. The output of the query is then compared to the provided expected data via a `MINUS` sql operation.
- Only tested with BigQuery 

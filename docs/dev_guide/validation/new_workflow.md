# Great Expectations Workflow

## Starting up the Jupyter Server

Run the `make` command 

```$ make serve_great_expectations_jupyterlab```

to start up the jupyter lab server where you can create and edit expectations and checkpoints. You will see output like what is shown below. 

```bash
$ make serve_great_expectations_jupyterlab 
docker compose exec airflow-scheduler /bin/bash -c \
        "mkdir -p /opt/airflow/.jupyter/share/jupyter/runtime &&\
        cd /opt/airflow/great_expectations/ &&\
        jupyter lab --ip 0.0.0.0 --port 18888"
[I 2023-06-28 22:06:12.415 ServerApp] Package jupyterlab took 0.0000s to import
...
[I 2023-06-28 22:06:12.903 ServerApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
[W 2023-06-28 22:06:12.908 ServerApp] No web browser found: Error('could not locate runnable browser').
[C 2023-06-28 22:06:12.908 ServerApp] 
    
    To access the server, open this file in a browser:
        file:///opt/airflow/.jupyter/share/jupyter/runtime/jpserver-164217-open.html
    Or copy and paste one of these URLs:
        http://44a94924c5da:18888/lab?token=be72207c3a182bed5c026af4c3014765250e98e0f8994d9c
        http://127.0.0.1:18888/lab?token=be72207c3a182bed5c026af4c3014765250e98e0f8994d9c
```

Copy the URL starting with `http://127.0.0.1:18888/...` and paste it into a browser.

!!! note

    To copy the URL, highlight it, right click it, and select **Copy**. Don't press ctrl+c as that will shut down the jupyter server.

This will bring you to a jupyterlab interface where you can develop `GX` resources in the execution environment.

There are several [Expectation development workflows](https://docs.greatexpectations.io/docs/guides/expectations/create_expectations_overview). This demo shows the interactive workflow.

## Creating Expectations Interactively

In the **file browser** on the left, go into the `uncommitted` directory and create a new notebook.

Load the `DataContext` and our data_warehouse `Datasource`

```python
import great_expectations as gx

context = gx.get_context()
datasource = context.get_datasource(datasource_name="fluent_dwh_source")
```

Pick and load a `DataAsset` to develop `Expectations` for. You can view all registered `DataAssets` via `datasource.get_asset_names()`.

```python
data_asset_name = f"data_raw.temp_chicago_sidewalk_cafe_permits"
expectation_suite_name = f"{data_asset_name}_suite"
data_asset = datasource.get_asset(asset_name=data_asset_name)
```

\[Optional\]: Define a strategy for splitting the data into batches and sorting those data batches. In this example, I've split the data by year and month based on a `date`-valued column in the dataset.

```python
data_asset.add_splitter_year_and_month(column_name="issued_date")
data_asset.add_sorters(["+year", "+month"])
```

Get a `Validator` instance for the current `DataAsset` and inspect a few rows of a batch.


```python
validator = context.get_validator(
    batch_request=data_asset.build_batch_request(),
    expectation_suite_name=expectation_suite_name,
)
validator.head()
# Or, if you want to explore a full batch
# sample_df = validator.head(fetch_all=True)
```

|  | location_state | expiration_date | zip_code | address_number_start | issued_date | city | location_zip | police_district |latitude | state | location_address | location_city | payment_date | longitude | account_number | site_number | ward | doing_business_as_name | street_type | permit_number | address | legal_name | address_number | street | street_direction| geometry | source_data_updated | ingestion_check_time |
| ---: | :--- | :--- | ---: | ---: | :--- | :--- | :--- | ---: | ---: | :--- | :--- | :--- | :--- | ---: | ---: | ---: | ---: | :--- | :--- | ---: | :--- | :--- | ---: | :--- | :--- | :--- | :--- | :--- |
| 0 | | 2024-02-29 00:00:00 | 60647 | 0 | 2023-06-22 00:00:00 | CHICAGO | | | | IL | | | 2023-06-22 00:00:00 | | 426327 | 1 | 1 | MINI MOTT | BLVD| 1829707 | 0 W LOGAN BLVD| MINI MOTT CO. | 0 | LOGAN | W | 0101000020E6100000000000000000F87F000000000000F87F | 2023-06-24T09:48:47Z | 2023-06-25T03:50:03.938039Z |
| 1 | | 2024-02-29 00:00:00 | 60618 | 2890 | 2023-06-09 00:00:00 | CHICAGO | | 14 | 41.9338 | IL | | | 2023-06-09 00:00:00 | -87.7154 | 432506 |1 | 35 | LA CELIA LATIN KITCHEN| AVE | 1829005 | 2890 N MILWAUKEE AVE | LA CELIA LLC | 2890 | MILWAUKEE | N | 0101000020E6100000B3EEF24EC8ED55C0DF3844EB85F74440 | 2023-06-24T09:48:47Z | 2023-06-25T03:50:03.938039Z |
| 2 | | 2024-02-29 00:00:00 | 60610 | 0 | 2023-06-15 00:00:00 | CHICAGO | |1 | 41.8821 | IL | | | 2023-06-15 00:00:00 | -87.628 | 478579 |1 | 42 | Cafe Sophie| ST | 1828623 | 0 N STATE ST | CAFE SOPHIE GOLD COAST LLC | 0 | STATE | N | 0101000020E61000007A196F8E30E855C0698D6D16E8F04440 | 2023-06-24T09:48:47Z | 2023-06-25T03:50:03.938039Z |
| 3 | | 2024-02-29 00:00:00 | 60622 | 0 | 2023-06-23 00:00:00 | CHICAGO | | 18 | 41.9041 | IL | | | 2023-06-23 00:00:00 | -87.6287 |7533 |1 | 26 | LA BRUQUENA RESTAURANT & LOUNGE | ST | 1828620 | 0 W DIVISION ST| LA BRUQUENA RESTAURANT & LOUNGE, INC. | 0 | DIVISION | W | 0101000020E61000005EFC06633DE855C096906BEDB7F34440 | 2023-06-24T09:48:47Z | 2023-06-25T03:50:03.938039Z |
| 4 | | 2024-02-29 00:00:00 | 60657 | 3328 | 2023-06-22 00:00:00 | CHICAGO | | 19 | 41.9424 | IL | | | 2023-06-22 00:00:00 | -87.6707 | 482604 |2 | 32 | PWU DUMMY ACCOUNT | AVE | 1827202 | 3328 N LINCOLN AVE| PWU DUMMY ACCOUNT|3328 | LINCOLN| N | 0101000020E6100000B1ABBBF9ECEA55C0A31D5016A0F84440 | 2023-06-24T09:48:47Z | 2023-06-25T03:50:03.938039Z |

Now that we have a `Validator` instance, we can use its `expect_*` methods to start defining expectations. To view the available `Expectation` types, run `validator.list_available_expectation_types()`. I've included a sample below. (Note: some expectations aren't implemented for all sources)

```python
[
    'expect_column_distinct_values_to_be_in_set',
    'expect_column_distinct_values_to_contain_set',
    'expect_column_distinct_values_to_equal_set',
    'expect_column_kl_divergence_to_be_less_than',
    'expect_column_mean_to_be_between',
    'expect_column_median_to_be_between',
    'expect_column_most_common_value_to_be_in_set',
    'expect_column_pair_values_a_to_be_greater_than_b',
    'expect_column_pair_values_to_be_equal',
    'expect_column_pair_values_to_be_in_set',
    'expect_column_proportion_of_unique_values_to_be_between',
    'expect_column_quantile_values_to_be_between',
    'expect_column_sum_to_be_between',
    'expect_column_to_exist',
    'expect_column_unique_value_count_to_be_between',
    'expect_column_value_lengths_to_be_between',
     ...
    'expect_column_value_z_scores_to_be_less_than',
    'expect_column_values_to_be_between',
    'expect_column_values_to_be_in_set',
    'expect_column_values_to_be_in_type_list',
    'expect_column_values_to_not_be_null',
    'expect_column_values_to_be_unique',
    'expect_column_values_to_match_regex',
    'expect_column_values_to_match_regex_list',
    'expect_compound_columns_to_be_unique',
    'expect_multicolumn_sum_to_equal',
    'expect_table_column_count_to_be_between',
    'expect_table_columns_to_match_ordered_list',
    'expect_table_columns_to_match_set',
    'expect_table_row_count_to_be_between',
    'expect_table_row_count_to_equal_other_table'
]
```

To inspect the `args` and/or `kwargs` for a method, prefix the call with a question mark to see the docstring (or two question marks to see the docstring and source code).

```python
?validator.expect_column_values_to_match_json_schema

Signature: validator.expect_column_values_to_match_json_schema(*args, **kwargs)
Docstring:
Expect the column entries to be JSON objects matching a given JSON schema.

expect_column_values_to_match_json_schema is a [Column Map Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_map_expectations).

Args:
    column (str):       The column name.
    json_schema (str):  The JSON schema (in string form) to match

Keyword Args:
    mostly (None or a float between 0 and 1):  Successful if at least mostly fraction of values match the expectation. For more detail, see [mostly](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#mostly).
 ...
```


```python
checkpoint_name = f"{data_asset_name}_checkpoint"
```



## Useful Great Expectations Commands

```bash
default@container_id:/opt/airflow/great_expectations$ great_expectations suite list
```

### Editing a suite

Enter this command to generate a notebook that will help edit expectations. Use the `-nj` flag to prevent `great_expectations` from starting up another jupyter server (you can just access the newly created notebook in the `/expectations` directory).

```bash
default@cfade96635e4:/opt/airflow/great_expectations$ great_expectations suite edit data_raw.temp_chicago_homicide_and_shooting_victimizations.warning -nj
```

In the suite editor notebook, each expectation in the suite will have its own cell that you can run, inspect, and edit. At the end of the notebook, expectations in the `validator` object will be written to file.

#### Removing an expectation

While reviewing expectations, you may find an expectation you want to just remove. You can delete such expectations via the `validator` object's `.remove_expectation()` method, but you have to pass in the expectation that is to be removed from the suite. You can access the expectation by accessing the `.expectation_config` attr of the `ExpectationValidationResult`.


```python
expectation_validation_result = validator.expect_column_values_to_match_regex(
    column='victimization_fbi_cd',
    mostly=1.0,
    regex='-?\d+',
    meta={
        'profiler_details': {
            'evaluated_regexes': {
                '(?:25[0-5]|2[0-4]\\d|[01]\\d{2}|\\d{1,2})(?:.(?:25[0-5]|2[0-4]\\d|[01]\\d{2}|\\d{1,2})){3}': 0.0,
                '-?\\d+': 1.0, '-?\\d+(?:\\.\\d*)?': 1.0, '<\\/?(?:p|a|b|img)(?: \\/)?>': 0.0,
                '[A-Za-z0-9\\.,;:!?()\\"\'%\\-]+': 1.0,
                '\\b[0-9a-fA-F]{8}\\b-[0-9a-fA-F]{4}-[0-5][0-9a-fA-F]{3}-[089ab][0-9a-fA-F]{3}-\\b[0-9a-fA-F]{12}\\b ': 0.0,
                '\\d+': 1.0,
                '\\s+$': 0.0,
                '^\\s+': 0.0,
                'https?:\\/\\/(?:www\\.)?[-a-zA-Z0-9@:%._\\+~#=]{2,255}\\.[a-z]{2,6}\\b(?:[-a-zA-Z0-9@:%_\\+.~#()?&//=]*)': 0.0
            },
            'success_ratio': 1.0
        }
    }
)
```

To confirm that you've removed that expectation, check the length of the expectation suite (via the command below) before and after removing the expectation.

```python
print(f"Number of expectations pre-removal: {len(validator.expectation_suite.expectations)}")
validator.remove_expectation(expectation_validation_result.expectation_config)
print(f"Number of expectations after-removal: {len(validator.expectation_suite.expectations)}")
```
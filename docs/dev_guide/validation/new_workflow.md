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
[I ServerApp] Package jupyterlab took 0.0000s to import
...
[I ServerApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
[W ServerApp] No web browser found: Error('could not locate runnable browser').
[C ServerApp] 
    
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

??? note "How-to: View all `DataFrame` columns"

    If your dataset has more than 20 columns (`pandas`' default `max_columns` value), `validator.head()` will show a truncated view of the `DataFrame`. To view all columns, change the `max_columns` setting for your notebook by running this:

    ```python
    import pandas as pd
    pd.options.display.max_columns = 0
    ```

|  | location_state | expiration_date | zip_code | address_number_start | issued_date | city | location_zip | police_district |latitude | state | location_address | location_city | payment_date | longitude | account_number | site_number | ward | doing_business_as_name | street_type | permit_number | address | legal_name | address_number | street | street_direction| geometry | source_data_updated | ingestion_check_time |
| ---: | :--- | :--- | ---: | ---: | :--- | :--- | :--- | ---: | ---: | :--- | :--- | :--- | :--- | ---: | ---: | ---: | ---: | :--- | :--- | ---: | :--- | :--- | ---: | :--- | :--- | :--- | :--- | :--- |
| 0 | | 2024-02-29 00:00:00 | 60647 | 0 | 2023-06-22 00:00:00 | CHICAGO | | | | IL | | | 2023-06-22 00:00:00 | | 426327 | 1 | 1 | MINI MOTT | BLVD| 1829707 | 0 W LOGAN BLVD| MINI MOTT CO. | 0 | LOGAN | W | 0101000020E6100000000000000000F87F000000000000F87F | 2023-06-24T09:48:47Z | 2023-06-25T03:50:03.938039Z |
| 1 | | 2024-02-29 00:00:00 | 60618 | 2890 | 2023-06-09 00:00:00 | CHICAGO | | 14 | 41.9338 | IL | | | 2023-06-09 00:00:00 | -87.7154 | 432506 |1 | 35 | LA CELIA LATIN KITCHEN| AVE | 1829005 | 2890 N MILWAUKEE AVE | LA CELIA LLC | 2890 | MILWAUKEE | N | 0101000020E6100000B3EEF24EC8ED55C0DF3844EB85F74440 | 2023-06-24T09:48:47Z | 2023-06-25T03:50:03.938039Z |
| 2 | | 2024-02-29 00:00:00 | 60610 | 0 | 2023-06-15 00:00:00 | CHICAGO | |1 | 41.8821 | IL | | | 2023-06-15 00:00:00 | -87.628 | 478579 |1 | 42 | Cafe Sophie| ST | 1828623 | 0 N STATE ST | CAFE SOPHIE GOLD COAST LLC | 0 | STATE | N | 0101000020E61000007A196F8E30E855C0698D6D16E8F04440 | 2023-06-24T09:48:47Z | 2023-06-25T03:50:03.938039Z |
| 3 | | 2024-02-29 00:00:00 | 60622 | 0 | 2023-06-23 00:00:00 | CHICAGO | | 18 | 41.9041 | IL | | | 2023-06-23 00:00:00 | -87.6287 |7533 |1 | 26 | LA BRUQUENA RESTAURANT & LOUNGE | ST | 1828620 | 0 W DIVISION ST| LA BRUQUENA RESTAURANT & LOUNGE, INC. | 0 | DIVISION | W | 0101000020E61000005EFC06633DE855C096906BEDB7F34440 | 2023-06-24T09:48:47Z | 2023-06-25T03:50:03.938039Z |
| 4 | | 2024-02-29 00:00:00 | 60657 | 3328 | 2023-06-22 00:00:00 | CHICAGO | | 19 | 41.9424 | IL | | | 2023-06-22 00:00:00 | -87.6707 | 482604 |2 | 32 | PWU DUMMY ACCOUNT | AVE | 1827202 | 3328 N LINCOLN AVE| PWU DUMMY ACCOUNT|3328 | LINCOLN| N | 0101000020E6100000B1ABBBF9ECEA55C0A31D5016A0F84440 | 2023-06-24T09:48:47Z | 2023-06-25T03:50:03.938039Z |

Now that we have a `Validator` instance, we can use its `expect_*` methods to start defining expectations.

??? note "How-to: View available `Expectation` types"

    Run command `validator.list_available_expectation_types()` to get a list of `Expectation` methods available through the `Validator` instance (as shown below). Note: some expectations aren't implemented for all sources; check [here](https://docs.greatexpectations.io/docs/reference/expectations/implemented_expectations/) to review which `Expectation`s are implemented for each backend.

    `validator.list_available_expectation_types()`

    ```python
    [
        'expect_column_distinct_values_to_be_in_set',
        'expect_column_distinct_values_to_contain_set',
        'expect_column_distinct_values_to_equal_set',
        'expect_column_kl_divergence_to_be_less_than',
        'expect_column_max_to_be_between',
        'expect_column_mean_to_be_between',
        'expect_column_median_to_be_between',
        'expect_column_min_to_be_between',
        'expect_column_most_common_value_to_be_in_set',
        'expect_column_pair_values_a_to_be_greater_than_b',
        'expect_column_pair_values_to_be_equal',
        'expect_column_pair_values_to_be_in_set',
        'expect_column_proportion_of_unique_values_to_be_between',
        'expect_column_quantile_values_to_be_between',
        'expect_column_stdev_to_be_between',
        'expect_column_sum_to_be_between',
        'expect_column_to_exist',
        'expect_column_unique_value_count_to_be_between',
        'expect_column_value_lengths_to_be_between',
        'expect_column_value_lengths_to_equal',
        'expect_column_value_z_scores_to_be_less_than',
        'expect_column_values_to_be_between',
        'expect_column_values_to_be_dateutil_parseable',
        'expect_column_values_to_be_decreasing',
        'expect_column_values_to_be_in_set',
        'expect_column_values_to_be_in_type_list',
        'expect_column_values_to_be_increasing',
        'expect_column_values_to_be_json_parseable',
        'expect_column_values_to_be_null',
        'expect_column_values_to_be_of_type',
        'expect_column_values_to_be_unique',
        'expect_column_values_to_match_json_schema',
        'expect_column_values_to_match_like_pattern',
        'expect_column_values_to_match_like_pattern_list',
        'expect_column_values_to_match_regex',
        'expect_column_values_to_match_regex_list',
        'expect_column_values_to_match_strftime_format',
        'expect_column_values_to_not_be_in_set',
        'expect_column_values_to_not_be_null',
        'expect_column_values_to_not_match_like_pattern',
        'expect_column_values_to_not_match_like_pattern_list',
        'expect_column_values_to_not_match_regex',
        'expect_column_values_to_not_match_regex_list',
        'expect_compound_columns_to_be_unique',
        'expect_multicolumn_sum_to_equal',
        'expect_select_column_values_to_be_unique_within_record',
        'expect_table_column_count_to_be_between',
        'expect_table_column_count_to_equal',
        'expect_table_columns_to_match_ordered_list',
        'expect_table_columns_to_match_set',
        'expect_table_row_count_to_be_between',
        'expect_table_row_count_to_equal',
        'expect_table_row_count_to_equal_other_table'
    ]
    ```

To inspect the `args` and/or `kwargs` for a method, prefix the call with a question mark to see the docstring (or two question marks to see the docstring and source code).

??? note "How-to: View args and kwargs for an `Expectation`"

    To view the docstring (which includes descriptions of args and kwargs), prefix the method with one question mark and run that cell (note: leave off the parentheses). To view the method's docstring and source code, use two question marks.
    
    `?validator.expect_column_values_to_be_of_type`

    ```python
    Signature: validator.expect_column_values_to_be_of_type(*args, **kwargs)
    Docstring:
    Expect a column to contain values of a specified data type.

    expect_column_values_to_be_of_type is a [Column Map Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_map_expectations) for typed-column backends, and also for PandasDataset where the column dtype and provided type_ are unambiguous constraints (any dtype except 'object' or dtype of 'object' with     type_ specified as 'object').

    For PandasDataset columns with dtype of 'object' expect_column_values_to_be_of_type will
    independently check each row's type.

    Args:
        column (str):  The column name.
        type\_ (str):  A string representing the data type that each column should have as entries.
                        Valid types are defined by the current backend implementation and are
                        dynamically loaded. For example, valid types for PandasDataset include any
                        numpy dtype values (such as 'int64') or native python types (such as 'int'),
                        whereas valid types for a SqlAlchemyDataset include types named by the current
                        driver such as 'INTEGER' in most SQL dialects and 'TEXT' in dialects such as
                        postgresql. Valid types for SparkDFDataset include 'StringType', 'BooleanType'
                        and other pyspark-defined type names. Note that the strings representing these
                        types are sometimes case-sensitive. For instance, with a Pandas backend
                        `timestamp` will be unrecognized and fail the expectation, while `Timestamp`
                        would pass with valid data.

    Keyword Args:
        mostly (None or a float between 0 and 1):   Successful if at least mostly fraction of values match the expectation. For more detail, see [mostly](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#mostly).
    ...
    ```

Add `Expectations` to your `Validator`-instance's `expectation_suite` (or edit them if they already exist) by running the relevant `validator` method. Except for the four [standard arguments](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#catch_exceptions) (`result_format`, `catch_exceptions`, `meta`, and `mostly`), each `Expectation`-type can have different arguments (although they're pretty intuitive).


```python
validator.expect_table_columns_to_match_set(
    column_set=[
        "account_number", "site_number", "permit_number", "legal_name", "doing_business_as_name",
        "issued_date", "expiration_date", "payment_date", "address_number", "address_number_start",
        "street_direction", "street", "street_type", "city", "state", "zip_code", "address",
        "ward", "police_district", "location_state", "location_zip", "location_address",
        "location_city", "longitude", "latitude", "geometry", "source_data_updated",
        "ingestion_check_time"
    ],
    exact_match=True
)
validator.expect_column_distinct_values_to_be_in_set(column="state", value_set=["IL"])
validator.expect_column_distinct_values_to_be_in_set(column="city", value_set=["CHICAGO"])

validator.expect_column_values_to_be_null(column="location_address")
validator.expect_column_values_to_be_null(column="location_city")
validator.expect_column_values_to_be_null(column="location_state")
validator.expect_column_values_to_be_null(column="location_zip")

validator.expect_column_values_to_not_be_null(column="issued_date")
validator.expect_column_values_to_not_be_null(column="expiration_date")
validator.expect_column_values_to_not_be_null(column="legal_name")
validator.expect_column_values_to_not_be_null(column="doing_business_as_name")
validator.expect_column_values_to_not_be_null(column="source_data_updated")
validator.expect_column_values_to_not_be_null(column="ingestion_check_time")
validator.expect_column_values_to_not_be_null(column="permit_number")
validator.expect_column_values_to_not_be_null(column="account_number")
validator.expect_column_values_to_not_be_null(column="zip_code", mostly=0.99)

validator.expect_column_values_to_be_of_type(column="issued_date", type_="TIMESTAMP")
validator.expect_column_values_to_be_of_type(column="expiration_date", type_="TIMESTAMP")
validator.expect_column_values_to_be_of_type(column="payment_date", type_="TIMESTAMP")

validator.expect_column_values_to_be_unique(column="permit_number")
validator.expect_column_values_to_be_unique(column="account_number")
validator.expect_column_values_to_be_unique(column="site_number")

validator.expect_column_unique_value_count_to_be_between(
    column="ward",
    min_value=1,
    max_value=50,
    meta={
        "notes": {
            "format": "markdown",
            "content": (
                "Some markdown-formatted comment about this expectation fo be included in the "
                + "Data Docs entry for this validation. **Markdown** `Supported`, $\latex$ too."
            ),
        }
    }
)
...
```

??? note "Standard argument example: `result_format`"

    The valid `result_format` types are:

    * "BASIC" (default)
    * "BOOLEAN_ONLY"
    * "SUMMARY"
    * "COMPLETE"

    ### "BASIC"

    ```python
    validator.expect_column_value_lengths_to_be_between(
        column="doing_business_as_name",
        min_value=1,
        max_value=127,
        result_format="BASIC"
    )
    ```
    outputs
    ```python
    {
        "meta": {},
        "success": true,
        "result": {
            "element_count": 58,
            "unexpected_count": 0,
            "unexpected_percent": 0.0,
            "partial_unexpected_list": [],
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_percent_total": 0.0,
            "unexpected_percent_nonmissing": 0.0
        },
        "exception_info": {
            "raised_exception": false,
            "exception_traceback": null,
            "exception_message": null
        }
    }
    ```

    ### "BOOLEAN_ONLY"

    ```python
    validator.expect_column_value_lengths_to_be_between(
        column="doing_business_as_name",
        min_value=1,
        max_value=127,
        result_format="BOOLEAN_ONLY"
    )
    ```
    outputs
    ```python
    {
        "meta": {},
        "success": true,
        "result": {},
        "exception_info": {
            "raised_exception": false,
            "exception_traceback": null,
            "exception_message": null
        }
    }
    ```

    ### "SUMMARY"

    ```python
    validator.expect_column_value_lengths_to_be_between(
        column="doing_business_as_name",
        min_value=1,
        max_value=127,
        result_format="SUMMARY"
    )
    ```
    outputs
    ```python
    {
        "meta": {},
        "success": true,
        "result": {
            "element_count": 58,
            "unexpected_count": 0,
            "unexpected_percent": 0.0,
            "partial_unexpected_list": [],
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_percent_total": 0.0,
            "unexpected_percent_nonmissing": 0.0,
            "partial_unexpected_counts": []
        },
        "exception_info": {
            "raised_exception": false,
            "exception_traceback": null,
            "exception_message": null
        }
    }
    ```

    ### "COMPLETE"

    ```python
    validator.expect_column_value_lengths_to_be_between(
        column="doing_business_as_name",
        min_value=1,
        max_value=127,
        result_format="COMPLETE"
    )
    ```
    outputs
    ```python
    {
        "meta": {},
        "success": true,
        "result": {
            "element_count": 58,
            "unexpected_count": 0,
            "unexpected_percent": 0.0,
            "partial_unexpected_list": [],
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_percent_total": 0.0,
            "unexpected_percent_nonmissing": 0.0,
            "partial_unexpected_counts": [],
            "unexpected_list": [],
            "unexpected_index_query": "SELECT doing_business_as_name \nFROM data_raw.temp_chicago_sidewalk_cafe_permits \nWHERE doing_business_as_name IS NOT NULL AND NOT (length(doing_business_as_name) >= 1 AND length(doing_business_as_name) <= 127);"
        },
        "exception_info": {
            "raised_exception": false,
            "exception_traceback": null,
            "exception_message": null
        }
    }
    ```

When you're content with your suite of `Expectations`, save them to your `DataContext`.

```python
validator.save_expectation_suite(discard_failed_expectations=False)
```

Now you can add (or update) a `Checkpoint` to evaluate that suite of `Expectations`. This step can take a while (depending on the size of the table and the number of `Expectations` being evaluated).

```python
checkpoint = context.add_or_update_checkpoint(
    name=f"{data_asset_name}_checkpoint",
    validator=validator,
)
checkpoint_result = checkpoint.run()
```

After running the `Checkpoint`, report the results (by building Data Docs)

```python
context.build_data_docs()
```







#### Removing an expectation

While reviewing expectations, you may find an expectation you want to just remove. You can delete such expectations via the `validator` object's `.remove_expectation()` method, but you have to pass in the expectation that is to be removed from the suite. You can access the expectation by accessing the `.expectation_config` attr of the `ExpectationValidationResult`.


```python
expectation_validation_result = validator.expect_column_values_to_not_be_null(column="zip_code")

print(f"Expectation configs in suite pre-removal: {len(validator.expectation_suite.expectations)}")
validator.remove_expectation(
    expectation_configuration=expectation_validation_result.expectation_config
)
print(f"Expectation configs in suite post-removal: {len(validator.expectation_suite.expectations)}")
```

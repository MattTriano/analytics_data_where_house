# Developing a Suite of Expectations Manually

To manually develop your suite of expectations, get a shell in the `py-utils` container and `cd` into the `great_expectations/` directory

```bash
user@host:~/...$ make get_py_utils_shell 
docker-compose up -d py-utils
cc_real_estate_dbt_airflow_ge_py-utils_1 is up-to-date
docker-compose exec py-utils /bin/bash
root@b5a3b6c2727f:/home# cd great_expectations/
```

Enter `great_expectations suite new` to start making a new suite of expectations and enter **1** to select the manual workflow. 

```bash
root@b5a3b6c2727f:/home/great_expectations# great_expectations suite new
Using v3 (Batch Request) API

How would you like to create your Expectation Suite?
    1. Manually, without interacting with a sample Batch of data (default)
    2. Interactively, with a sample Batch of data
    3. Automatically, using a Data Assistant
: 1
```

When prompted for a name for your suite of expectations, use this format

```python
suite_name = f"{schema_name}.{table_name}.warning"
```

So if the `table_name` is **temp_chicago_shotspotter_alerts** and the table is in the **data_raw** schema, the `suite_name` will be **data_raw.temp_chicago_shotspotter_alerts.warning** and output the expectation suite to the file `.../expectations/data_raw/temp_chicago_shotspotter_alerts/warning.json`.

When prompted, enter that suite name. `great_expectations` will initialize the `.json` file and an `.ipynb` notebook file for this suite, and then spin up a jupyter lab server so you can add expectations via that notebook file.

```bash
Name the new Expectation Suite [warning]: data_raw.temp_chicago_shotspotter_alerts.warning
Opening a notebook for you now to edit your expectation suite!
If you wish to avoid this you can add the `--no-jupyter` flag.

[I ServerApp] jupyter_server_terminals | extension was successfully linked.
[I ServerApp] jupyterlab | extension was successfully linked.
...
...
[I ServerApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
[W ServerApp] No web browser found: could not locate runnable browser.
[C ServerApp] 
    
    To access the server, open this file in a browser:
        file:///root/.local/share/jupyter/runtime/jpserver-290-open.html
    Or copy and paste one of these URLs:
        http://b5a3b6c2727f:18888/lab?token=ebd251c23bd05cbdba8240305fc469a1314955ffb1438802
     or http://127.0.0.1:18888/lab?token=ebd251c23bd05cbdba8240305fc469a1314955ffb1438802
```

Go to either of those URLs and open the `edit_<suite_name>.ipynb` notebook. 

The first cell will:

1. import necessary importables (not shown),
2. load this project's [DataContext](https://docs.greatexpectations.io/docs/terms/data_context),
3. set the `expectation_suite_name` variable, and
4. check if there's expectation suite file with that name:
    * if `True`: load that expectation suite into the `suite` variable.
    * if `False`: create a new and empty suite of expectations with that name.

```python
context = ge.data_context.DataContext()
# Feel free to change the name of your suite here.
#   Renaming this will not remove the other one.
expectation_suite_name = "data_raw.temp_chicago_shotspotter_alerts.warning"
try:
    suite = context.get_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    print(
        f'Loaded ExpectationSuite "{suite.expectation_suite_name}" ' +
        f'containing {len(suite.expectations)} expectations.'
    )
except DataContextError:
    suite = context.create_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    print(f'Created ExpectationSuite "{suite.expectation_suite_name}".')
```

## Creating Expectations

Now that you have an `ExpectationSuite` object, you can start defining and adding specific `Expectations`. The generated notebook provides headings for **Table Expectation(s)** and **Column Expectation(s)** where you can configure `Expectations` from the [`Expectation` Gallery](https://greatexpectations.io/expectations/) to reflect your expectations for the data table or specific columns.

All `Expectations` will have at least [these three optional arguments](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/):

* [`result_format`](https://docs.greatexpectations.io/docs/reference/expectations/result_format): A string or dict specifying which fields to return when the `Expectation` is evaluated.
* `catch_exceptions`: A boolean that determines how `great_expectations` shouve when an error or exception is raised during 
evaluation of the `Expectation`.
* `meta`: A dict of user-supplied metadata to be stored with the `Expectation`.

`ColumnMapExpectation`s and `MultipleColumnMapExpectation`s also have another standard (optional) argument, `mostly`.

* `mostly`: A float between 0 and 1 (inclusive) indicating what proportion of column values must meet the `Expectation`.


### Table Expectations

`TableExpectations` are, unsurprisingly, expectations for table-level properties. At the time of writing, there are eight core `TableExpectations`. I set these `TableExpectations` in nearly every `ExpectationSuite` I develop:

* `expect_table_row_count_to_be_between`: Expect the number of rows to be between two values.
* `expect_table_columns_to_match_set`: Expect the columns to match an unordered set.

On occasion, a few of the remaining six may be worth adding.

* `expect_table_columns_to_match_ordered_list`: Expect the columns to exactly match a specified list.
* `expect_column_to_exist`: Expect the specified column to exist.
* `expect_table_row_count_to_equal_other_table`: Expect the number of rows to equal the number in another table.
* `expect_table_column_count_to_be_between`: Expect the number of columns to be between two values.
* `expect_table_column_count_to_equal`: Expect the number of columns to equal a value.
* `expect_table_row_count_to_equal`: Expect the number of rows to equal a value.

#### Defining a `TableExpectation`

In the notebook, you can define a `TableExpectation` by creating an `ExpectationConfiguration` instance with parameters:

* `expectation_type`: the name of the `TableExpectation`,
* `kwargs`: a `dict` of keyword arguments that enable you to represent your expectations for data values (the available kwargs can be found in the `Expectation`'s documentation from the Gallery).

```python
# Create an Expectation
expectation_configuration = ExpectationConfiguration(
    # Name of expectation type being added
    # expectation_type="expect_table_columns_to_match_set",
    # These are the arguments of the expectation
    # The keys allowed in the dictionary are Parameters and
    # Keyword Arguments of this Expectation Type
    kwargs={
        "column_list": [
            "account_id", "user_id", "transaction_id",
            "transaction_type", "transaction_amt_usd"
        ]
    },
    # This is how you can optionally add a comment about this expectation.
    # It will be rendered in Data Docs.
    # See this guide for details:
    # `How to add comments to Expectations and display them in Data Docs`.
    meta={
        "notes": {
            "format": "markdown",
            "content": """
                Some clever comment about this expectation.
                **Markdown** `Supported`
            """
        }
    }
)
```

Then add that expectation to your `suite`

```python
# Add the Expectation to the suite
suite.add_expectation(expectation_configuration=expectation_configuration)
```

### Column Expectations

There are far more `Column`-based `Expectations` (especially if you include user-contributed `Expectations` in the count). Here are some of the most generally applicable ones:

#### For Categorically-valued columns:
* [`expect_column_distinct_values_to_be_in_set`](https://greatexpectations.io/expectations/expect_column_distinct_values_to_be_in_set)
* [`expect_column_distinct_values_to_equal_set`](https://greatexpectations.io/expectations/expect_column_distinct_values_to_equal_set)

#### For Numerically-valued columns:

* [`expect_column_mean_to_be_between`](https://greatexpectations.io/expectations/expect_column_mean_to_be_between)
* [`expect_column_median_to_be_between`](https://greatexpectations.io/expectations/expect_column_median_to_be_between)
* [`expect_column_most_common_value_to_be_in_set`](https://greatexpectations.io/expectations/expect_column_most_common_value_to_be_in_set)

#### For String-valued columns:
* [`expect_column_value_lengths_to_be_between`](https://greatexpectations.io/expectations/expect_column_value_lengths_to_be_between)
* [`expect_column_values_to_match_regex`](https://greatexpectations.io/expectations/expect_column_values_to_match_regex)

#### For Date[Time]-valued columns:
* [`expect_column_values_to_be_dateutil_parseable`](https://greatexpectations.io/expectations/expect_column_values_to_be_dateutil_parseable)
* [`expect_column_values_to_match_strftime_format`](https://greatexpectations.io/expectations/expect_column_values_to_match_strftime_format)

#### Regarding columns properties:
* [`expect_column_values_to_be_unique`](https://greatexpectations.io/expectations/expect_column_values_to_be_unique)
* [`expect_column_values_to_be_of_type`](https://greatexpectations.io/expectations/expect_column_values_to_be_of_type)
* [`expect_column_values_to_not_be_null`](https://greatexpectations.io/expectations/expect_column_values_to_not_be_null)

Check the `Expectation` Gallery to review all available features.

#### Defining a `ColumnExpectation`
As with `TableExpectations`, setting `ColumnExpectations` for a column will 

```python
expectation_configuration = ExpectationConfiguration(
    expectation_type="expect_column_value_lengths_to_be_between",
    kwargs={
        "column": "zip_code",
        "min_value": 5,
        "max_value": 5,
    }
)
suite.add_expectation(expectation_configuration=expectation_configuration)
```

## Saving and Reviewing Expectations

After setting expectations, run the last cell to

1. Print out the expectations in your `expectation_suite`,
2. save your `expectation_suite` to file,
3. generate data docs for your `expectation_suite`, and 
4. unsuccessfully attempt to open those data docs.
    * You should be able to find them in the location `/airflow/great_expectations/uncommitted/data_docs/local_site/expectations/<your_expectation_suite's_name>.html`.

```python
print(
    context.get_expectation_suite(expectation_suite_name=expectation_suite_name)
)
context.save_expectation_suite(
    expectation_suite=suite,
    expectation_suite_name=expectation_suite_name
)

suite_identifier = ExpectationSuiteIdentifier(
    expectation_suite_name=expectation_suite_name
)
context.build_data_docs(resource_identifiers=[suite_identifier])
context.open_data_docs(resource_identifier=suite_identifier)
```

At this point, you're ready to set up a `Checkpoint` for your suite.


## Resources
* [Official doc](https://docs.greatexpectations.io/docs/guides/expectations/how_to_create_and_edit_expectations_based_on_domain_knowledge_without_inspecting_data_directly/) on manual expectation setting.
* The [Expectation Gallery](https://greatexpectations.io/expectations/)
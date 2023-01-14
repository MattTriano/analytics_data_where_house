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
# Feel free to change the name of your suite here. Renaming this will not remove the other one.
expectation_suite_name = "data_raw.temp_chicago_shotspotter_alerts.warning"
try:
    suite = context.get_expectation_suite(expectation_suite_name=expectation_suite_name)
    print(
        f'Loaded ExpectationSuite "{suite.expectation_suite_name}" containing {len(suite.expectations)} expectations.'
    )
except DataContextError:
    suite = context.create_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    print(f'Created ExpectationSuite "{suite.expectation_suite_name}".')
```
# Data Validation with `great_expectations`
## Setting up New Data Sources

This project already configures a [`great_expectations` Datasource](https://docs.greatexpectations.io/docs/terms/datasource/) and [Data Connectors](https://docs.greatexpectations.io/docs/terms/data_connector) for the included `dwh_db` database, but if you want to set up another Datasource (ie a connection to another data source), you can interactively set up and test a configuration via the following steps:

start the `py-utils` service's container and `cd` into the `great_expectations/` directory

```bash
make get_py_utils_shell
...
root@<container_id>:/home# cd great_expectations/
```

Then enter this to bring up Datasource-configuration prompts and enter values as appropriate. The example below shows steps for configuring another PostgreSQL Datasource.

```bash
root@<container_id>:/home/great_expectations# great_expectations datasource new
Using v3 (Batch Request) API

What data would you like Great Expectations to connect to?
    1. Files on a filesystem (for processing with Pandas or Spark)
    2. Relational database (SQL)
: 2

Which database backend are you using?
    1. MySQL
    2. Postgres
    3. Redshift
    4. Snowflake
    5. BigQuery
    6. Trino
    7. other - Do you have a working SQLAlchemy connection string?
: 2

Because you requested to create a new Datasource, we'll open a notebook for you now to complete it!
[NotebookApp] Serving notebooks from local directory: /home/great_expectations/uncommitted
[NotebookApp] Jupyter Notebook 6.5.2 is running at:
[NotebookApp] http://<container_id>:18888/?token=<a_long_token_string>
[NotebookApp]  or http://127.0.0.1:18888/?token=<a_long_token_string>
```

Go to either of the jupyter URLs shown and open the just-created notebook file named something similar to `datasource_new.ipynb`. Edit cells as as appropriate following the instructions in the notebook.

Run the **Test Your Datasource Configuration** cell to test the configuration. You might have to enter plaintext credentials in this notebook and then replace the plaintext strings with the name of the appropriate environment variable after writing the configuration to the `great_expectations.yml` file (eg for the `password:` field, replace the actual password with `${SOURCE_PASSWORD_NAME_IN_.env_file}`).

After testing indicates the connection works, run the last cell to add the configuration to the `great_expectations.yml` config file in `/airflow/great_expectations/`. **Note:** Replace any plaintext credential strings with variables before committing the file to source control.



## Generating a Suite of Expectations for a Data Set

To use `great_expectations`'s Data Assistant to generate a suite of expectations for a data set interactively, first start the `py-utils` service's container and `cd` into the `great_expectations/` directory

```bash
make get_py_utils_shell
...
root@<container_id>:/home# cd great_expectations/
```

Then enter this to bring up suite-generation prompts

```bash
root@<container_id>:/home/great_expectations# great_expectations suite new
```

At this prompt, enter 3 to use the Data Assistant to automatically generate some expectations (after you specify which columns to ignore in a notebook)

```bash
How would you like to create your Expectation Suite?
    1. Manually, without interacting with a sample Batch of data (default)
    2. Interactively, with a sample Batch of data
    3. Automatically, using a Data Assistant
: 3
```

Then select the data set to set expectations for

```bash
Which data asset (accessible by data connector "default_inferred_data_connector_name") would you like to use?
...
    10. data_raw.cook_county_parcel_locations
    11. data_raw.cook_county_parcel_sales
    12. data_raw.cook_county_parcel_value_assessments
...
Type [n] to see the next page or [p] for the previous. When you're ready to select an asset, enter the index.
: 11
```

and use the default name by pressing enter and entering `y` when asked

```bash
Name the new Expectation Suite [data_raw.cook_county_parcel_sales.warning]:

Great Expectations will create a notebook, containing code cells that select from available columns in your dataset and
generate expectations about them to demonstrate some examples of assertions you can make about your data.

When you run this notebook, Great Expectations will store these expectations in a new Expectation Suite "data_raw.cook_county_parcel_sales.warning" here:

  file:///home/great_expectations/expectations/data_raw/cook_county_parcel_sales/warning.json

Would you like to proceed? [Y/n]: y
```

Now that a data set is selected, `great_expectations` will generate a notebook for your suite and spin up a jupyter server on port 18888 (which is mapped to port 18888 on the host system). In a browser, go to either of URLs in the output and open the notebook named `edit_{the default name of the suite from the last step}.ipynb`.

```bash
Opening a notebook for you now to edit your expectation suite!
If you wish to avoid this you can add the `--no-jupyter` flag.


[NotebookApp] Serving notebooks from local directory: /home/great_expectations/uncommitted
[NotebookApp] Jupyter Notebook 6.5.2 is running at:
[NotebookApp] http://<container_id>:18888/?token=<a_long_token_string>
[NotebookApp]  or http://127.0.0.1:18888/?token=<a_long_token_string>
[NotebookApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
```

In the notebook, run the first code cell without changes (maybe increase the batch size if you want better initial expectations, although it will take longer to generate those initial expectations).

In the second code cell, you'll indicate the columns to exclude from the automatic expectation generation process. I find it's easier to just comment out column names and run the sell so that the `exclude_column_names` variable is defined (and set equal to an empty list).

Then run the remaining two code cells. The third code cell determines values for each of the initial expectations for each columns, and the fourth cell formats the expectations into `json` and writes them to a file in the location indicated in the suite naming step.

At this point, you can exit out of the notebook and delete it if you want. These expectations are intentionally not production-ready and some will fail if/when you try to use them to validate the full data set, so you'll have to review and edit these expectations while configuring a Checkpoint.

Those expectations are in the `.json` file in a subdirectory of the `./airflow/great_expectations/expectations/` directory, and the relative path will be the name given to the expectation suite (ie `.../expectations/data_raw/cook_county_parcel_sales/warning.json`).

## Configuring a Checkpoint and Validating a Data Set

Note: If you've just generated your suite of expectations (ie if the notebook server is still up), shut down the notebook server without exiting the `py-utils` container. If things don't shut down nicely, enter `jupyter notebook stop 18888` to free up port 18888.

In the `py-utils` container, you can generate a new checkpoint via `great_expectations checkpoint new <some_descriptive_name>`. Checkpoints can run one or more suite of expectations, so this project will name checkpoints via the convention `data_set_schema.data_set_table_name`. So for the expectation suite generated in the [above section](#generating-a-suite-of-expectations-for-a-data-set), command below will name the checkpoint and start up a jupyter server

```bash
root@c7cd3e337ddf:/home/great_expectations# great_expectations checkpoint new data_raw.cook_county_parcel_sales
Using v3 (Batch Request) API
Because you requested to create a new Checkpoint, we'll open a notebook for you now to edit it!
If you wish to avoid this you can add the `--no-jupyter` flag.


[NotebookApp] Serving notebooks from local directory: /home/great_expectations/uncommitted
[NotebookApp] Jupyter Notebook 6.5.2 is running at:
[NotebookApp] http://<container_id>:18888/?token=<a_long_token_string>
[NotebookApp]  or http://127.0.0.1:18888/?token=<a_long_token_string>
[NotebookApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
```

Open the notebook named `edit_checkpoint_{data_set_schema.data_set_table_name}.ipynb` and run the first code cell to run import statments and load the data context.

The next data cell formats the checkpoint's configs. Look over the contents and confirm that it names the right table (data_asset), expectation suite, data source, etc. If anything looks off and you want to see the other valid options, run cells three and four. After making changes (I had to change both the `data_asset_name` and `expectation_suite_name`) the config for my SimpleCheckpoint looked like: 

```python
my_checkpoint_name = "data_raw.cook_county_parcel_sales" # This was populated from your CLI command.

yaml_config = f"""
name: {my_checkpoint_name}
config_version: 1.0
class_name: SimpleCheckpoint
run_name_template: "%Y%m%d-%H%M%S-my-run-name-template"
validations:
  - batch_request:
      datasource_name: where_house_source
      data_connector_name: default_inferred_data_connector_name
      data_asset_name: data_raw.cook_county_parcel_sales
      data_connector_query:
        index: -1
    expectation_suite_name: data_raw.cook_county_parcel_sales.warning
"""
```

When you're happy with the config, run that code cell to set the configs for your checkpoint and then run the **Test your Checkpoint Configuration** code cell to see if your config is valid. If it is (it will print out `... Successfully instantiated SimpleCheckpoint.`), the next cell allows you to review your Checkpoint and running the **Add your Checkpoint** cell will actually save the Checkpoint.

If you want to run the validation checkpoint and generate data docs with the results of the checks, uncomment and run the last code cell. Those data docs can be found
* on the host machine in:
  * `.../airflow/great_expectations/uncommitted/data_docs/local_site/validations/data_raw/cook_county_parcel_sales/warning/<%Y%m%d-%H%M%S>-my-run-name-template/<%Y%m%d-%H%M%S.%fZ>/<hash-looking-string>.html`
* In the jupyter notebook tree, at:
  * `/data_docs/<same_as_on_host_past__data_docs>.html`

That data docs page will show you which expectations failed and allow you to review all of the expectations. You can manualy edit those expectations in the suite's `.json` file, or run `great_expectations suite edit data_raw.cook_county_parcel_sales.warning` (replace the suite as appropriate) at the command line in the `py-utils` container to interactively edit the suite.

After reviewing your expectations editing or removing the unreasonable ones, you can rerun your checkpoint via a command like

```bash
great_expectations checkpoint run data_raw.cook_county_parcel_sales
```
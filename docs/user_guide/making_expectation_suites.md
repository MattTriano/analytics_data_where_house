# Data Validation with `great_expectations`

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
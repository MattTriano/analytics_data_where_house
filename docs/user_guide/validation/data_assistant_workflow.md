## Generating a Suite of Expectations via the Data Assistant

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
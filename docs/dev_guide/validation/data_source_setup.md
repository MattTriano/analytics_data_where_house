# Setting up New Data Sources

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
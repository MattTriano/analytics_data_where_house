# Analytics Data Where House

This platform automates curation of a local data warehouse of interesting up-to-date public data sets and enables users (well, mainly one user; me) to easily add data sets to the warehouse, build analyses that explore and answer questions with that regularly updating data, and discover existing assets to accelerate exploring new questions.

At present, it uses docker to provision and run:
* a PostgreSQL + PostGIS database as the data warehouse,
* a pgAdmin4 database administration interface,
* Airflow components to orchestrate tasks (note: uses a LocalExecutor),
* dbt to manage data transformation and cleaning tasks, serve and facilitate search of the data dictionary and catalog, and
* custom python code that makes it easy to implement an ELT pipeline for [any other table hosted by Socrata](http://www.opendatanetwork.com/).

## Motivation
I like to do my research before I buy anything, especially if it's a big ticket item. I've been considering buying a house for a while, but the methods I use for answering questions like "what phone should I buy?" or "how can I make my apartment less drafty in winter" haven't been adequate to answer the questions I have about real estate. Fortunately, the real estate market I've grown fond of has the richest public data culture in the US (that I, a data scientist focused on Chicago-related issues, am aware of), and this market's Assessor's Office regularly [publishes data](https://datacatalog.cookcountyil.gov/browse?tags=cook%20county%20assessor) I can mine for answers to some of my biggest questions.

## Socrata Table Ingestion Flow

The Update-data DAGs for (at least) Socrata tables follow the pattern below:
* Check the metadata of the table's data source (via [api](https://socratametadataapi.docs.apiary.io/) if available, or if not, by [scraping](https://www2.census.gov/) where possible)
  * If the local data warehouse's data is stale:
    * download and ingest all new records into a temporary table,
    * identify which records are new or are updates to prior records, and
    * add any new or updated records to a running table of all distinct records
  * If the local data warehouse's data is as fresh as the source:
    * update the freshness-check-metadata table and end

<p align="center" width="100%">
 <img src="imgs/Socrata_ELT_DAG_tgs_condensed_w_statuses.PNG" width="80%" alt="Simple Update DAG Flow"/>
</p>

Before downloading potentially gigabytes of data, we check the data source's metadata to determine if the source data has been updated since the most recent successful update of that data in the local data warehouse. Whether there is fresh data or not, we'll log the results of that check in the data_warehouse's `metadata.table_metadata` table. 

<p align="center" width="100%">
 <img src="imgs/Socrata_ELT_DAG_metadata_check_taskgroup_expanded.PNG" width="80%" alt="check_table_metadata TaskGroup"/>
</p>

<p align="center" width="100%">
 <img src="imgs/metadata_table_query_view.PNG" width="80%" alt="Freshness check metadata Table in pgAdmin4"/>
</p>

If the data source's data is fresher than the data in the local data warehouse, the system downloads the full table from the data source (to a file in the Airflow-scheduler container) and then runs the `load_data_tg` TaskGroup, which:
1. loads it into a "temp" table (via the appropriate data-loader TaskGroup),
2. creates a persisting table for this data set in the `data_raw` schema if the data set is a new addition to the warehouse,
3. compares all records from the latest data set (in the "temp" table) against all records previously added to the persisting `data_raw` table for all original columns, selects records that are completely new or are updated versions of prior records, and appends those new or updated records to the persisting `data_raw` table.
  * Note: updated records do not replace the prior records here. All distinct versions are kept so that it's possible to examine changes to a record over time.
4. The `metadata.table_metadata` table is updated to indicate the table in the local data warehouse was successfully updated on this freshness check.

<p align="center" width="100%">
 <img src="imgs/Socrata_ELT_DAG_load_data_taskgroup_expanded_ingestions_condensed.PNG" width="80%" alt="load_data_tg TaskGroup loaders minimized"/>
</p>

Tables with geospatial features/columns will be downloaded in the .geojson format (which has a much more flexible structure than .csv files), while tables without geospatial features (ie flat tabular data) will be downloaded as .csv files. Different code is needed to correctly and efficiently read and ingest these different formats. So far, this platform has implemented data-loader TaskGroups to handle .geojson and .csv file formats, but this pattern is easy to extend if other data sources only offer other file formats.

<p align="center" width="100%">
 <img src="imgs/Socrata_ELT_DAG_load_data_taskgroup_and_ingestion_task_groups_expanded.PNG" width="80%" alt="data-loading TaskGroups in load_data_tg TaskGroup"/>
</p>

Many public data tables are exported from production systems, where records represent something that can change over time. For example, in this [building permit table](https://data.cityofchicago.org/Buildings/Building-Permits/ydr8-5enu), each record represents an application for a building permit, and rather than adding a new record any time the process moved forward (e.g. a fee was paid, or a contact was added, or the permit gets issued), the original record gets updated. After this data is updated, the prior state of the table is gone (or at least no longer publicly available), which is ideal for people involved in the process who have to look up the current status of a permit request, but for someone seeking to learn about the process, it's useful to keep the original record as well as all distinct updates to the record.

This system is built to have this updating strategy for tables in the `data_raw` schema, and this query shows the count of new or updated records grouped by the data-publication datetime when the record was new to the local data warehouse.

<p align="center" width="100%">
 <img src="imgs/Count_of_records_after_update.PNG" width="80%" alt="Counts of distinct records in data_raw table by when the source published that data set version"/>
</p>

## Usage

After the [system is set up](#system-setup), you can easily add a Socrata data set to the warehouse by

1. Define the `SocrataTable` in `/airflow/dags/sources/tables.py`:

Look up the table's [documentation page](https://datacatalog.cookcountyil.gov/Property-Taxation/Assessor-Parcel-Sales/wvhk-k5uv) on the web and get the `table_id` from the URL (it will be nine characters long, all lowercase and with a hyphen in the middle). Use that `table_id` value, along with a sensible name for the table and [cron expressions](https://crontab.cronhub.io/) that indicates how frequently the system should check for data updates) to define a SocrataTable instance for the table.

```python
COOK_COUNTY_PARCEL_SALES = SocrataTable(
    table_id="wvhk-k5uv",
    table_name="cook_county_parcel_sales",
    schedule="0 6 4 * *",
    clean_schedule="30 6 4 * *",
)
```
Note: It's a convention in python to capitalize the names of constants, and as the `table_id` and `table_name` for a data set should be constant, I use the capitalized `table_name` as the name of the data set's `SocrataTable` instance variable.


2. Create a DAG in a file in `/airflow/dags/` based on the `update_data_raw_cook_county_parcel_sales` DAG below:

After copying the code into a new file, you only have to make changes to the 4 lines numbered below:
1: Replace `COOK_COUNTY_PARCEL_SALES` with the name of the `SocrataTable` instance variable from `tables.py`,
2: change the tags to reflect this data set,
3: change the name of this DAG's function name to reflect this data set, and
4: call that DAG function.

```python
# This is the full file /airflow/dags/cook_county/update_raw_cook_county_parcel_sales.py
import datetime as dt
import logging

from airflow.decorators import dag

from tasks.socrata_tasks import update_socrata_table
from sources.tables import COOK_COUNTY_PARCEL_SALES as SOCRATA_TABLE   ### 1.

task_logger = logging.getLogger("airflow.task")


@dag(
    schedule=SOCRATA_TABLE.schedule,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["cook_county", "parcels", "fact_table", "data_raw"],        ### 2.
)
def update_data_raw_cook_county_parcel_sales():                       ### 3.
    update_1 = update_socrata_table(
        socrata_table=SOCRATA_TABLE,
        conn_id="dwh_db_conn",
        task_logger=task_logger,
    )
    update_1
update_data_raw_cook_county_parcel_sales()                            ### 4.
```

Congratulations! You just defined a new data pipeline! After you unpause and run this DAG in the Airflow Web UI, the system will automatically build that data pipeline, add that data set to the warehouse, and update that data set on the schedule indicated in the `SocrataTable` instance.

## System Setup

Preprequisites:
To use this system, Docker is the only absolutely necessary prerequisite.

Having `GNU make` and/or core python on your host system will enable you to use included `makefile` recipes and scripts to streamline setup and common operations, but you could get by without them (although you'll have to figure more out).

### Setting up credentials
After cloning this repo and `cd`ing into your local, run this `make` command and respond to prompts the the requested values,

```bash
make make_credentials
```

#### Generating a Frenet Key to use as env var AIRFLOW__CORE__FERNET_KEY
To get a proper frenet key for the `AIRFLOW__CORE__FERNET_KEY` environment variable, the best way I know of involves the `cryptography` module, which isn't a built-in python module, but it is pretty common and it's easy enough to `pip install` or `conda install` into a `venv` or `conda env` if it hasn't already been installed as a dependency for something else.

```python
from cryptography.fernet import Fernet

fernet_key = Fernet.generate_key()
print(fernet_key.decode()) # your fernet_key
```
then copy that value and paste it into the appropriate field in the `.env` file in the same directory as this README.md file.


### Initializing the system

On the first startup of the system (and after setting your credentials), run the commands below to
1. build the platform's docker images, and initialize the airflow metadata database,
2. start up the system in detached mode (so that you don't have to open another terminal), and
3. create the `metadata` and `data_raw` schemas and the `metadata.table_metadata` table in your data warehouse database.

```bash
user@host:.../your_local_repo$ make initialize_system
user@host:.../your_local_repo$ make quiet_startup
user@host:.../your_local_repo$ make create_warehouse_infra
```

These commands only need to be run on first startup (although you will need to run `make build_images` to rebuild images if you make any changes to any of the `Dockerfile`s or add/remove packages from a `requirements.txt` file).

### Starting up the system

Run this command to startup the platform

```bash
user@host:.../your_local_repo$ make startup
```

After systems have started up, you can access:
* The pgAdmin4 database administration UI at [http://localhost:5678](http://localhost:5678)
  * Log in using the `PGADMIN_DEFAULT_EMAIL` and `PGADMIN_DEFAULT_PASSWORD` credentials from your `.env` file. 
* The Airflow UI at [http://localhost:8080](http://localhost:8080)
  * Log in using the `_AIRFLOW_WWW_USER_USERNAME` and `_AIRFLOW_WWW_USER_PASSWORD` credentials from your `.env` file.

### Setting up database connections in pgAdmin4

The pgAdmin4 UI makes it very easy to explore your data, inspect database internals, and make manual changes while developing features, but before you can make use of this excellent interface, you have to set a connection to a database. This platform uses two separate databases: one as a backend for Airflow, and the other as the data warehouse database.

To create a new connection, start by clicking the "Add New Server" button (you might have to click the "Servers" line in the lefthand tray first). On the **Connection** page, enter the appropriate credential values from your `.env` file,

<p align="center" width="100%">
  <img src="imgs/Setting_up_pgAdmin4_connection_to_airflow_metadata_pg2.PNG" width="90%" alt="Airflow metadata db connection"/>
</p>


and on the **General** tab, enter a display name for that connection (**airflow_metadata_db** shown)

<p align="center" width="100%">
  <img src="imgs/Setting_up_pgAdmin4_connection_to_airflow_metadata_pg1.PNG" width="60%" alt="Airflow metadata db general"/>
</p>

Repeat the process to connect to the data warehouse database, using the appropriate (and different from above) credential values from your `.env` file,

<p align="center" width="100%">
  <img src="imgs/Setting_up_pgAdmin4_connection_to_data_warehouse_db_pg1.PNG" width="45%" alt="Data Warehouse General"/>
 <img src="imgs/Setting_up_pgAdmin4_connection_to_data_warehouse_db_pg2.PNG" width="45%" alt="Data Warehouse Connection"/>
</p>

### Developing DAGs

DAGs put or developed in the `/<repo>/airflow/dags/` directory will quickly be available through the web UI and can be manually triggered or run there.

At present, a local mount is created at `/<repo>/data_raw` (host-side) to `/opt/airflow/data_raw` (container-side), so changes you make to a DAG from your host machine will be (nearly immediately) available you can develop.

### Serving dbt Data Documentation and Discovery UI 

To generate and serve documentation for the data transformations executed by dbt, run the command below, and after the doc server has started up, go to [http://localhost:18080](http://localhost:18080) to explore the documentation UI.

The documentation will be mainly based on the sources, column names, and descriptions recorded in the `.yml` file in the `.../dbt/models/...` directories with table-or-view-producing dbt scripts.

```bash
user@host:.../your_local_repo$ make serve_dbt_docs

```
<p align="center" width="100%">
  <img src="imgs/dbt_doc_sample_page_w_lineage_graph.PNG" width="90%" alt="dbt documentation page with table lineage graph"/>
</p>

### Specifying, installing, and updating dbt packages
 
Create a file named `packages.yml` in your dbt project directory and specify any packages you want to use in your project in the format shown below (or as shown in the [documentation](https://docs.getdbt.com/docs/build/packages))

```yml
packages:
  - package: dbt-labs/dbt_utils
    version: 0.9.2
```

Then, after specifying packages and versions to use, run this command to install packages.

```bash
user@host:.../your_local_repo$ make update_dbt_packages
01:33:04  Running with dbt=1.3.0
01:33:05  Installing dbt-labs/dbt_utils
01:33:05    Installed from version 0.9.2
01:33:05    Up to date!
```

## Developing queries and exploring data in pgAdmin4

pgAdmin4 is a very feature-rich environment and makes it very convenient to test out queries or syntax and see the result.

<p align="center" width="100%">
  <img src="imgs/Geospatial_query_and_data_in_pgAdmin4.PNG" width="90%" alt="pgAdmin4's geospatial query viewer"/>
</p>

## Generating a Suite of Expectations for a Data Set

To use `great_expectations`'s Data Assistant to interactively generate a suite of expectations for a data set, first start the `py-utils` service's container and `cd` into the `great_expectations/` directory

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

And at this prompt, enter the number of the data set you want to generate expectations for.

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

## Configuring a Checkpoint and Validating a Data Set

Note: If you've just generated your suite of expectations (ie if the notebook server is still up), shut down the notebook server. If things don't shut down nicely, enter `jupyter notebook stop 18888` to free up port 18888.

## Troubleshooting Notes

While developing workflows, occassionally I'll run into permissions issues where Airflow tries to create things in a location that was created automatically outside of the specified volume locations or something and I've had to change (take) ownership of the location (from outside of the container) via a `chown` command like the one below (where I'm `-R`ecursively taking ownership of the `dbt/` directory).

```bash
sudo chown -R $USER:$USER dbt/
```

Additionally, if you run into issues while debugging a dbt model where you're making changes to the model but getting the same error every time, try running the command below (to clean out the previously compiled dbt models and installed packages, then reinstall packages) and run the relevant DAG again to see if things update.

```bash
make clean_dbt
```

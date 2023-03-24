# Using the system

## Adding a new pipeline

The [Socrata platform](https://www.opendatanetwork.com/) is a wealth of public data and this system only requires a few manual steps to set up an ELT pipeline that adds a data set to your local warehouse.  

### Manual Steps

1. Add a `SocrataTable` instance with the data set's table_id and table_name to the `/airflow/dags/sources/tables.py` file as shown [here](adding_a_socrata_pipeline.md#adding-a-data-set-to-tables) 

2. Copy this [DAG](adding_a_socrata_pipeline.md#make-a-dag-file) into a file anywhere in `/airflow/dags/` and edit the 4 annotated lines.

3. [In the [Airflow Web UI](http://localhost:8080)] Run that new DAG.

4. (Optional) To check data quality before updating your local table, [set expectations for the data](validation/index.md).

5. (Optional) To [standardize column names, dtypes, or order](standardizing_columns.md) for a data set, edit the file named `{table_name}_standardized.sql` in directory `/airflow/dbt/models/standardized/`.


### Workflow Overview

The workflow for producing usable tables follows this pattern:

1. (`data_raw` schema): Set up an ingestion pipeline.

    1.1. Extract data to a local file.

    1.2. Load that data into a "temp" table.
    
    1.3. Select distinct records that aren't already in the warehouse and add them to a persistant table.
    
    1.4. Define a suite of expectations to validate future data updates.

2. (`standardize` schema): Implement dbt models that standardize the data set's columns.

    2.1. [Standardize column](standardizing_columns.md) [names, dtypes, order] and perform cleaning steps in the `f"{data_set_name}_standardized.sql` dbt model file.

3. (`clean` schema): Automatically generates dbt models that implement a deduplication strategy, produce a clean data set.

4. (`feature` schema): Implement dbt models to engineer data features.

    3.1. [Engineer desired features](feature_engineering/index.md)

5. (`dwh` schema): Implement dbt models to assemble data into analytically useful tables.

For tables hosted by Socrata, this system reduces steps 1.1 through 1.3 to a [3 minute operation](/user_guide/adding_a_socrata_pipeline), generates a nearly ready `..._standardized.sql` stub for 2.1, and automatically produces the `..._clean.sql` file from 2.2 after the `..._standardized.sql` stub is edited.

![check_table_metadata TaskGroup](/assets/imgs/Socrata_ELT_DAG/dbt_intermediate_model_generation_tasks.png)


## Adding a new Pipeline for a table hosted by Socrata

After the [system is set up](/setup/getting_started), you can easily add a Socrata data set to the warehouse by

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

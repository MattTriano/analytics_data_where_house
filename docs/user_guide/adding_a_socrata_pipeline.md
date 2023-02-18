<a name="adding-a-data-set-to-tables"></a>
## 1. Adding a new Pipeline for a table hosted by Socrata

After the [system is set up](/setup/getting_started), you can easily add a Socrata data set to the warehouse by

Define the `SocrataTable` in `/airflow/dags/sources/tables.py`:

Look up the table's [documentation page](https://datacatalog.cookcountyil.gov/Property-Taxation/Assessor-Parcel-Sales/wvhk-k5uv) on the web and get the `table_id` from the URL (it will be nine characters long, all lowercase and with a hyphen in the middle).

```python
SAMPLE_DATA_SET = SocrataTable(
    table_id="wvhk-k5uv",             # (1)
    table_name="sample_data_set",   # (2)
    schedule="0 6 4 * *",             # (3)
    )
```

1. A Socrata table's `table_id` will always be 9 characters long and consist of two blocks of 4 characters (numbers or lowercase letters) separated by a hyphen. You can find the `table_id` in the data documentation URL or export link for the data set.
2. Ideally the name of the `SocrataTable` instance should be the uppercased `table_name` (which should be lowercase).
3. This sets the update frequency. If you aren't familiar with the `crontab` format, use [cron expressions](https://crontab.cronhub.io/).

<a name="make-a-dag-file"></a>
## 2. Create a DAG for the data set

Copy this code into a new file in `/airflow/dags/` and edit the 4 annotated lines for the new data set:

```python
import datetime as dt
import logging

from airflow.decorators import dag

from tasks.socrata_tasks import update_socrata_table
from sources.tables import COOK_COUNTY_PARCEL_SALES as SOCRATA_TABLE   # (1)

task_logger = logging.getLogger("airflow.task")


@dag(
    schedule=SOCRATA_TABLE.schedule,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=["cook_county", "parcels", "fact_table", "data_raw"],        # (2)
)
def update_data_raw_sample_data_set():                                # (3)
    update_1 = update_socrata_table(
        socrata_table=SOCRATA_TABLE,
        conn_id="dwh_db_conn",
        task_logger=task_logger,
    )
    update_1
update_data_raw_sample_data_set()                                     # (4)
```

1.  Replace `COOK_COUNTY_PARCEL_SALES` with the name of the `SocrataTable` instance variable from `tables.py`.
2. Change the tags to reflect this data set.
3. Change the name of this DAG's function name to reflect this data set.
4. Call that DAG function.
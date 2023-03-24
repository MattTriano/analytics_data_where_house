## Socrata Table Ingestion Flow

The Update-data DAGs for (at least) Socrata tables follow the pattern below:
* Check the metadata of the table's data source (via [api](https://socratametadataapi.docs.apiary.io/) if available, or if not, by [scraping](https://www2.census.gov/) where possible)
  * If the local data warehouse's data is stale:
    * download and ingest all new records into a temporary table,
    * identify new records and updates to prior records, and
    * add any new or updated records to a running table of all distinct records
  * If the local data warehouse's data is as fresh as the source:
    * update the freshness-check-metadata table and end

![Simple Update DAG Flow](/assets/imgs/Socrata_ELT_DAG/High_level_update_socrata_table_view_w_task_statuses.PNG)

Before downloading potentially gigabytes of data, we check the data source's metadata to determine if the source data has been updated since the most recent successful update of that data in the local data warehouse. Whether there is new data or not, we'll log the results of that check in the data_warehouse's `metadata.table_metadata` table. 

![check_table_metadata TaskGroup](/assets/imgs/Socrata_ELT_DAG/Check_table_metadata_tg.PNG)

![Freshness check metadata Table in pgAdmin4](/assets/imgs/metadata_table_query_view.PNG)

If the data source's data is fresher than the data in the local data warehouse, the system downloads the entire table from the data source (to a file in the Airflow-scheduler container) and then runs the `load_data_tg` TaskGroup, which:

1. Loads it into a "temp" table (via the appropriate data-loader TaskGroup).

    ![load_data_tg TaskGroup loaders minimized](/assets/imgs/Socrata_ELT_DAG/Condensed_file_ext_loader_tgs.PNG)

2. Creates a persisting table for this data set in the `data_raw` schema if the data set is a new addition to the warehouse.

3. Checks if the initial dbt data_raw deduplication model exists, and if not, the `make_dbt_data_raw_model` task automatically generates a data-set-specific dbt data_raw model file.

    ![load_data_tg TaskGroup data_raw table-maker and dbt model generator](/assets/imgs/Socrata_ELT_DAG/schema_and_file_generation_phase_of_load_data_tg.PNG)

4. Compares all records from the latest data set (in the "temp" table) against all records previously added to the persisting `data_raw` table. Records that are entirely new or are updates of prior records (i.e., at least one source column has a changed value) are appended to the persisting `data_raw` table.

    * Note: updated records do not replace the prior records here. All distinct versions are kept so that it's possible to examine changes to a record over time.

5. The `metadata.table_metadata` table is updated to indicate the table in the local data warehouse was successfully updated on this freshness check.

    ![load_data_tg TaskGroup metadata update](/assets/imgs/Socrata_ELT_DAG/Finishing_load_data_tg_metadata_update.PNG)

    Those tasks make up the `load_data_tg` Task Group.

    ![load_data_tg TaskGroup High Level](/assets/imgs/Socrata_ELT_DAG/High_level_load_data_tg.PNG)

If the local data warehouse has up-to-date data for a given data source, we will just record that finding in the metadata table and end the run.

![Local data is fresh so we will note that and end](/assets/imgs/Socrata_ELT_DAG/Local_data_is_fresh_condition.PNG)

### Data Loader task_groups

Tables with geospatial features/columns will be downloaded in the .geojson format (which has a much more flexible structure than .csv files), while tables without geospatial features (ie flat tabular data) will be downloaded as .csv files. Different code is needed to correctly and efficiently read and ingest these different formats. So far, this platform has implemented data-loader TaskGroups to handle .geojson and .csv file formats, but this pattern is easy to extend if other data sources only offer other file formats.

![data-loading TaskGroups in load_data_tg TaskGroup](/assets/imgs/Socrata_ELT_DAG/Full_view_data_loaders_in_load_data_tg.PNG)

Many public data tables are exported from production systems, where records represent something that can change over time. For example, in this [building permit table](https://data.cityofchicago.org/Buildings/Building-Permits/ydr8-5enu), each record represents an application for a building permit. Rather than adding a new record any time the application process moves forward (e.g., when a fee was paid, a contact was added, or the permit gets issued), the original record gets updated. After this data is updated, the prior state of the table is gone (or at least no longer publicly available). This is ideal for intended users of the production system (i.e., people involved in the process who have to look up the current status of a permit request). But for someone seeking to understand the process, keeping all distinct versions or states of a record makes it possible to see how a record evolved. So I've developed this workflow to keep the original record and all distinct updates for (non "temp_") tables in the `data_raw` schema.

This query shows the count of new or updated records grouped by the data-publication DateTime when the record was new to the local data warehouse.

![People love using fireworks on New Years](/assets/imgs/pgAdmin4/shot_spotter_alerts_after_new_years.png)

  **Fig:** People really love using fireworks on New Years, and those alerts typically get updated after review.


# Standardizing Column Names, dtypes, and order in the _standardized model

The `Update_raw_{data_set_name}` DAG will generate a dbt model file named `{data_set_name}_standardized.sql` in the dbt models directory `/airflow/dbt/models/intermediate/`. This is where you should change column names, dtypes, and order.

This file will have all of the data set's columns and their inferred dtypes, but you'll have to change at least the two **REPLACE_WITH_...** values before the model can be run. 

```python
{{ config(materialized='view') }}
{% set ck_cols = ["REPLACE_WITH_COMPOSITE_KEY_COLUMNS"] %}
{% set record_id = "REPLACE_WITH_BETTER_id" %}
WITH records_with_basic_cleaning AS (
	SELECT
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }}
		column_name_1::bigint AS column_name_1
		...
```

The `ck_cols` dbt variable should be a list of a set of column(s) that form a composite key that uniquely identifies a record across tables.

If the table comes with a natural (single) primary key column, use that in the `ck_cols` list and delete both `record_id` lines.

If it takes 2+ columns to uniquely identify a record, set `ck_cols` and `record_id` as shown (but leave the other `record_id` line alone). When choosing a string for the `record_id`, pick something that describes the unit of one table record (e.g. parcel_sale_id for parcel sales, or parcel_valuation_id for parcel assessments).

```python
{% set ck_cols = ["column_name_1", "column_name_2", ..., "column_name_n"] %}
{% set record_id = "(something_descriptive)_id" %}
```

## Selecting columns that uniquely identify a record 

The pgAdmin4 interface makes it very easy to execute queries and explore tables; right click any database object and open up the **Query tool**. There, you can enter and run queries. Here are some useful ones.

### Query Cookbook

#### See a few rows

To see **5** records from the table **table_name** in the **schema_name** schema, run query: 

```sql
SELECT *
FROM schema_name.table_name
LIMIT 5
```

Subsequent examples will use reference actual schemas and tables.

#### Number of records

This will return the number of records in the table.

```sql
SELECT COUNT(*)
FROM data_raw.temp_chicago_traffic_crashes
```

#### Distinct values in a column

This will return the number of records with a distinct value in the named column (**crash_record_id** shown)

```sql
SELECT COUNT(DISTINCT crash_record_id)
FROM data_raw.temp_chicago_traffic_crashes
```

If the table is large, `DISTINCT` is a bit slow. You can speed it up a little by changing to query:

```sql
SELECT COUNT(*)
FROM (
	SELECT DISTINCT crash_record_id
	FROM data_raw.temp_chicago_traffic_crashes
) AS temp
```

#### Records with duplicated values in a given column

When identifying a set of distinct columns, often one column (**crash_record_id** in this example) is highly unique by itself, but not perfectly unique. This query shows you all columns for the set of records with duplicated **crash_record_id** values. Looking through column values, look for the other features that changed. 

```sql
WITH distinct_colpair_count AS (
	SELECT crash_record_id, 
		   row_number() over(partition by crash_record_id) as rn
	FROM data_raw.temp_chicago_traffic_crashes
),
dupe_crash_ids AS (
	SELECT *
	FROM data_raw.chicago_traffic_crashes
	WHERE crash_record_id IN (
		SELECT crash_record_id
		FROM distinct_colpair_count
		WHERE rn > 1
	)
	ORDER BY crash_record_id
)

SELECT *
FROM dupe_crash_ids
```

To see the counts of each value in a column (`work_zone_type` shown)
```sql
SELECT count(*) AS row_count, work_zone_type 
FROM data_raw.chicago_traffic_crashes
GROUP BY work_zone_type
ORDER BY row_count
```

Throw in a `length()` feature if you want to see string lengths (useful if you want to specify the number of chars for a column)

```sql
SELECT count(*) AS row_count, length(work_zone_type) AS n_chars, work_zone_type 
FROM data_raw.chicago_traffic_crashes
GROUP BY work_zone_type
ORDER BY row_count
```

## Type Casting

Useful casting

* `<col_name>::smallint` Small Integers
* `<col_name>::double precision` Floats (decimal numerics)


## References:
* Thought process for the `_standardized` stage: [issue 55](https://github.com/MattTriano/analytics_data_where_house/issues/55) if you're interested in the  
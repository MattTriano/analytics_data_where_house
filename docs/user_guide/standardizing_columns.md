
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

#### Confirming a set of column(s) form a primary/composite key

Add one or more columns to the `(partition by ...)` list and run the query. If the returned count is 0, then the column set is a valid composite key (or primary key if there's only one column). Aim to use the smallest set of columns that returns zero.

```sql
WITH composite_key_candidates AS (
	SELECT row_number() over(partition by sale_document_num, pin) as rn
	FROM data_raw.cook_county_parcel_sales
)

SELECT count(*)
FROM composite_key_candidates
WHERE rn > 1
```

The `row_number() over(partition by col1, col2, ..., coln) as rn...` operation will partition the data into groups with every distinct combination of values in the named columns, and within each grouping, it will produce row numnbers (from 1 up to the number of records in that group). 

If the set of columns produces a set of values that can identify every distinct record, there shouldn't be any records where the row number (`rn`) is above 1.

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

#### Extracting date or time components from timestamp-valued columns

This is often useful when trying to evaluate whether a time-zone correction is needed or was incorrectly applied. From domain knowledge, I know that traffic is worst from 8am to 10am (when many are driving to work) and from 4pm to 6pm (when many are driving home). I reason that the rate of crashes will be proportional to the number of vehicles on the road, so the distribution of crash-counts-by-hour should show peaks around both rush hours and a minimum from ~4am to 6am (log after bars close but before most drivers leave for work). If the peaks and valley line up with the expected times, I know the data was published in Chicago time (which is UTC-5 or UTC-6 depending on daylight savings time). If the distribution showed a minimum around 10am and peaks 1pm and 10pm, I'd know the data was published as UTC-0.

```sql
WITH date_col AS (
	SELECT crash_date::timestamp AS crash_date
	FROM data_raw.chicago_traffic_crashes
)

SELECT count(*), extract(hour FROM crash_date) AS date_hour
FROM date_col
GROUP BY date_hour
ORDER BY date_hour
```

Check [the documentation](https://www.postgresql.org/docs/current/functions-datetime.html#FUNCTIONS-DATETIME-EXTRACT) to see what else `extract` can extract.


#### Zero or left-padding strings incorrectly parsed to a numeric type

If you know that the column values should all be strings of the same length but the leading values were ignored (often zeros or spaces), cast the column (or feature) to a text/char/varchar type and **left-pad** it with a padding character to a given length (shown using `0` as the char and 2 as the length).

```sql
SELECT lpad(extract(month FROM crash_date::timestamp)::char(2), 2, '0') AS crash_month
FROM data_raw.chicago_traffic_crashes
```

#### Removing punctuation or numbers from strings

If you want to replace any characters except for letters or spaces, use `regexp_replace()` with regex pattern `[^A-Z ]` (`^` means "except", and the `g` flag indicates "replace all matches").

```sql
WITH alpha_only AS (
	SELECT regexp_replace(upper(city::text), '[^A-Z ]', '', 'g') as city
	FROM data_raw.chicago_food_inspections
)

SELECT count(*), city
FROM alpha_only
GROUP BY city
ORDER BY count desc
```


## Type Casting

Useful casting

* `<col_name>::smallint` Small Integers (up to 2^16, or 65536)
* `<col_name>::int` Small Integers (up to 2^32, or 4294967296)
* `<col_name>::bigint` Small Integers (up to 2^64, or 1.8446744e+19)
* `<col_name>::double precision` Floats (decimal numerics)


## References:
* Thought process for the `_standardized` stage: [issue 55](https://github.com/MattTriano/analytics_data_where_house/issues/55) if you're interested in the  
{{ config(materialized='table') }}
{% set source_cols = [
     "pin", "year", "township_code", "class", "sale_date", "is_mydec_date", "sale_price",
     "sale_document_num", "sale_deed_type", "sale_seller_name", "is_multisale", "num_parcels_sale",
     "sale_buyer_name", "sale_type"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

-- selecting all records already in the full data_raw table
WITH records_in_data_raw_table AS (
     SELECT *, 1 AS retention_priority
     FROM {{ source('data_raw', 'cook_county_parcel_sales') }}
),

-- selecting all distinct records from the latest data pull (in the "temp" table)
current_pull_with_distinct_combos_numbered AS (
     SELECT *,
          row_number() over(partition by
               {% for sc in source_cols %}{{ sc }},{% endfor %}
               {% for mc in metadata_cols %}{{ mc }}{{ "," if not loop.last }}{% endfor %}
          ) as rn
     FROM {{ source('data_raw', 'temp_cook_county_parcel_sales') }}
),
distinct_records_in_current_pull AS (
     SELECT
          {% for sc in source_cols %}{{ sc }},{% endfor %}
          {% for mc in metadata_cols %}{{ mc }},{% endfor %}
          2 AS retention_priority
     FROM current_pull_with_distinct_combos_numbered
     WHERE rn = 1
),

-- stacking the existing data with all distinct records from the latest pull
data_raw_table_with_all_new_and_updated_records AS (
     SELECT *
     FROM records_in_data_raw_table
          UNION ALL
     SELECT *
     FROM distinct_records_in_current_pull
),

-- selecting records that where source columns are distinct (keeping the earlier recovery
--  when there are duplicates to chose from)
data_raw_table_with_new_and_updated_records AS (
     SELECT *,
      row_number() over(partition by
          {% for sc in source_cols %}{{ sc }}{{ "," if not loop.last }}{% endfor %}
          ORDER BY retention_priority
          ) as rn
     FROM data_raw_table_with_all_new_and_updated_records
),
distinct_records_for_data_raw_table AS (
     SELECT
          {% for sc in source_cols %}{{ sc }},{% endfor %}
          {% for mc in metadata_cols %}{{ mc }}{{ "," if not loop.last }}{% endfor %}
     FROM data_raw_table_with_new_and_updated_records
     WHERE rn = 1
)

SELECT *
FROM distinct_records_for_data_raw_table
ORDER BY pin, year, sale_document_num, source_data_updated

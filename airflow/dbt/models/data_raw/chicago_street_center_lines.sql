{{ config(materialized='table') }}
{% set source_cols = [
     "ewns_dir", "l_parity", "oneway_dir", "logiclf", "street_nam", "street_typ", "r_t_add",
     "l_censusbl", "r_parity", "r_zip", "edit_type", "tiered", "tnode_id", "edit_date",
     "create_tim", "suf_dir", "logicrf", "t_cross", "update_tim", "ewns", "objectid", "l_fips",
     "pre_dir", "logicrt", "status_dat", "f_zlev", "l_f_add", "r_f_add", "streetname",
     "flag_strin", "dir_travel", "ewns_coord", "status", "l_zip", "t_cross_st", "f_cross",
     "l_t_add", "t_zlev", "class", "length", "f_cross_st", "fnode_id", "create_use", "logiclt",
     "r_fips", "r_censusbl", "update_use", "shape_len", "trans_id", "geometry"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

-- selecting all records already in the full data_raw table
WITH records_in_data_raw_table AS (
    SELECT *, 1 AS retention_priority
    FROM {{ source('data_raw', 'chicago_street_center_lines') }}
),

-- selecting all distinct records from the latest data pull (in the "temp" table)
current_pull_with_distinct_combos_numbered AS (
     SELECT *,
          row_number() over(partition by
               {% for sc in source_cols %}{{ sc }},{% endfor %}
               {% for mc in metadata_cols %}{{ mc }}{{ "," if not loop.last }}{% endfor %}
          ) as rn
     FROM {{ source('data_raw', 'temp_chicago_street_center_lines') }}
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
ORDER BY objectid, source_data_updated

{{ config(materialized='table') }}
{% set source_cols = [
    "b25063_001e", "b25063_001ea", "b25063_001m", "b25063_001ma", "b25063_002e", "b25063_002ea",
    "b25063_002m", "b25063_002ma", "b25063_003e", "b25063_003ea", "b25063_003m", "b25063_003ma",
    "b25063_004e", "b25063_004ea", "b25063_004m", "b25063_004ma", "b25063_005e", "b25063_005ea",
    "b25063_005m", "b25063_005ma", "b25063_006e", "b25063_006ea", "b25063_006m", "b25063_006ma",
    "b25063_007e", "b25063_007ea", "b25063_007m", "b25063_007ma", "b25063_008e", "b25063_008ea",
    "b25063_008m", "b25063_008ma", "b25063_009e", "b25063_009ea", "b25063_009m", "b25063_009ma",
    "b25063_010e", "b25063_010ea", "b25063_010m", "b25063_010ma", "b25063_011e", "b25063_011ea",
    "b25063_011m", "b25063_011ma", "b25063_012e", "b25063_012ea", "b25063_012m", "b25063_012ma",
    "b25063_013e", "b25063_013ea", "b25063_013m", "b25063_013ma", "b25063_014e", "b25063_014ea",
    "b25063_014m", "b25063_014ma", "b25063_015e", "b25063_015ea", "b25063_015m", "b25063_015ma",
    "b25063_016e", "b25063_016ea", "b25063_016m", "b25063_016ma", "b25063_017e", "b25063_017ea",
    "b25063_017m", "b25063_017ma", "b25063_018e", "b25063_018ea", "b25063_018m", "b25063_018ma",
    "b25063_019e", "b25063_019ea", "b25063_019m", "b25063_019ma", "b25063_020e", "b25063_020ea",
    "b25063_020m", "b25063_020ma", "b25063_021e", "b25063_021ea", "b25063_021m", "b25063_021ma",
    "b25063_022e", "b25063_022ea", "b25063_022m", "b25063_022ma", "b25063_023e", "b25063_023ea",
    "b25063_023m", "b25063_023ma", "b25063_024e", "b25063_024ea", "b25063_024m", "b25063_024ma",
    "b25063_025e", "b25063_025ea", "b25063_025m", "b25063_025ma", "b25063_026e", "b25063_026ea",
    "b25063_026m", "b25063_026ma", "b25063_027e", "b25063_027ea", "b25063_027m", "b25063_027ma",
    "geo_id", "name", "state", "county", "tract", "dataset_base_url", "dataset_id"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

-- selecting all records already in the full data_raw table
WITH records_in_data_raw_table AS (
    SELECT *, 1 AS retention_priority
    FROM {{ source('data_raw', 'gross_rent_by_cook_county_il_tract') }}
),

-- selecting all distinct records from the latest data pull (in the "temp" table)
current_pull_with_distinct_combos_numbered AS (
    SELECT *,
        row_number() over(partition by
            {% for sc in source_cols %}{{ sc }},{% endfor %}
            {% for mc in metadata_cols %}{{ mc }}{{ "," if not loop.last }}{% endfor %}
        ) as rn
    FROM {{ source('data_raw', 'temp_gross_rent_by_cook_county_il_tract') }}
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

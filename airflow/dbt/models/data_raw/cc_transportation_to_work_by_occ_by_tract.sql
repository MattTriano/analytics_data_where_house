{{ config(materialized='table') }}
{% set source_cols = [
    "b08124_001e", "b08124_001ea", "b08124_001m", "b08124_001ma", "b08124_002e", "b08124_002ea",
    "b08124_002m", "b08124_002ma", "b08124_003e", "b08124_003ea", "b08124_003m", "b08124_003ma",
    "b08124_004e", "b08124_004ea", "b08124_004m", "b08124_004ma", "b08124_005e", "b08124_005ea",
    "b08124_005m", "b08124_005ma", "b08124_006e", "b08124_006ea", "b08124_006m", "b08124_006ma",
    "b08124_007e", "b08124_007ea", "b08124_007m", "b08124_007ma", "b08124_008e", "b08124_008ea",
    "b08124_008m", "b08124_008ma", "b08124_009e", "b08124_009ea", "b08124_009m", "b08124_009ma",
    "b08124_010e", "b08124_010ea", "b08124_010m", "b08124_010ma", "b08124_011e", "b08124_011ea",
    "b08124_011m", "b08124_011ma", "b08124_012e", "b08124_012ea", "b08124_012m", "b08124_012ma",
    "b08124_013e", "b08124_013ea", "b08124_013m", "b08124_013ma", "b08124_014e", "b08124_014ea",
    "b08124_014m", "b08124_014ma", "b08124_015e", "b08124_015ea", "b08124_015m", "b08124_015ma",
    "b08124_016e", "b08124_016ea", "b08124_016m", "b08124_016ma", "b08124_017e", "b08124_017ea",
    "b08124_017m", "b08124_017ma", "b08124_018e", "b08124_018ea", "b08124_018m", "b08124_018ma",
    "b08124_019e", "b08124_019ea", "b08124_019m", "b08124_019ma", "b08124_020e", "b08124_020ea",
    "b08124_020m", "b08124_020ma", "b08124_021e", "b08124_021ea", "b08124_021m", "b08124_021ma",
    "b08124_022e", "b08124_022ea", "b08124_022m", "b08124_022ma", "b08124_023e", "b08124_023ea",
    "b08124_023m", "b08124_023ma", "b08124_024e", "b08124_024ea", "b08124_024m", "b08124_024ma",
    "b08124_025e", "b08124_025ea", "b08124_025m", "b08124_025ma", "b08124_026e", "b08124_026ea",
    "b08124_026m", "b08124_026ma", "b08124_027e", "b08124_027ea", "b08124_027m", "b08124_027ma",
    "b08124_028e", "b08124_028ea", "b08124_028m", "b08124_028ma", "b08124_029e", "b08124_029ea",
    "b08124_029m", "b08124_029ma", "b08124_030e", "b08124_030ea", "b08124_030m", "b08124_030ma",
    "b08124_031e", "b08124_031ea", "b08124_031m", "b08124_031ma", "b08124_032e", "b08124_032ea",
    "b08124_032m", "b08124_032ma", "b08124_033e", "b08124_033ea", "b08124_033m", "b08124_033ma",
    "b08124_034e", "b08124_034ea", "b08124_034m", "b08124_034ma", "b08124_035e", "b08124_035ea",
    "b08124_035m", "b08124_035ma", "b08124_036e", "b08124_036ea", "b08124_036m", "b08124_036ma",
    "b08124_037e", "b08124_037ea", "b08124_037m", "b08124_037ma", "b08124_038e", "b08124_038ea",
    "b08124_038m", "b08124_038ma", "b08124_039e", "b08124_039ea", "b08124_039m", "b08124_039ma",
    "b08124_040e", "b08124_040ea", "b08124_040m", "b08124_040ma", "b08124_041e", "b08124_041ea",
    "b08124_041m", "b08124_041ma", "b08124_042e", "b08124_042ea", "b08124_042m", "b08124_042ma",
    "b08124_043e", "b08124_043ea", "b08124_043m", "b08124_043ma", "b08124_044e", "b08124_044ea",
    "b08124_044m", "b08124_044ma", "b08124_045e", "b08124_045ea", "b08124_045m", "b08124_045ma",
    "b08124_046e", "b08124_046ea", "b08124_046m", "b08124_046ma", "b08124_047e", "b08124_047ea",
    "b08124_047m", "b08124_047ma", "b08124_048e", "b08124_048ea", "b08124_048m", "b08124_048ma",
    "b08124_049e", "b08124_049ea", "b08124_049m", "b08124_049ma", "geo_id", "name", "state",
    "county", "tract", "dataset_base_url", "dataset_id"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

-- selecting all records already in the full data_raw table
WITH records_in_data_raw_table AS (
    SELECT *, 1 AS retention_priority
    FROM {{ source('data_raw', 'cc_transportation_to_work_by_occ_by_tract') }}
),

-- selecting all distinct records from the latest data pull (in the "temp" table)
current_pull_with_distinct_combos_numbered AS (
    SELECT *,
        row_number() over(partition by
            {% for sc in source_cols %}{{ sc }},{% endfor %}
            {% for mc in metadata_cols %}{{ mc }}{{ "," if not loop.last }}{% endfor %}
        ) as rn
    FROM {{ source('data_raw', 'temp_cc_transportation_to_work_by_occ_by_tract') }}
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

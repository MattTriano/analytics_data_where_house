{{ config(materialized='table') }}
{% set source_cols = [
    "nochangereasondescription", "changereasondescription", "bor_improvementvalue", "majorclass",
    "assessor_totalvalue", "appealseq", "appellant_zip", "pin_dash", "changereason",
    "attorney_firmname", "tax_year", "appellant_state", "taxcode", "appellant", "bor_totalvalue",
    "attorneycode", "rowid", "result", "attny", "ycoord_crs_3435", "bor_landvalue", "township_code",
    "appealtrk", "pin10", "long", "vol", "assessor_improvementvalue", "appealtype",
    "appellant_address", "class", "appealtypedescription", "appellant_city", "attorney_lastname",
    "xcoord_crs_3435", "appealid", "pin", "nochangereason", "assessor_landvalue", "lat",
    "attorney_firstname", "geometry"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

-- selecting all records already in the full data_raw table
WITH records_in_data_raw_table AS (
    SELECT *, 1 AS retention_priority
    FROM {{ source('staging', 'cook_county_parcel_assessment_appeals') }}
),

-- selecting all distinct records from the latest data pull (in the "temp" table)
current_pull_with_distinct_combos_numbered AS (
    SELECT *,
        row_number() over(partition by
            {% for sc in source_cols %}{{ sc }},{% endfor %}
            {% for mc in metadata_cols %}{{ mc }}{{ "," if not loop.last }}{% endfor %}
        ) as rn
    FROM {{ source('staging', 'temp_cook_county_parcel_assessment_appeals') }}
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

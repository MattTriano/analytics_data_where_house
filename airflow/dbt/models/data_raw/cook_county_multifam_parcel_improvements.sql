{{ config(materialized='table') }}
{% set source_cols = [
    "pin", "tax_year", "card_num", "class", "township_code", "cdu", "pin_is_multicard",
    "pin_num_cards", "pin_is_multiland", "pin_num_landlines", "year_built", "building_sqft",
    "land_sqft", "num_bedrooms", "num_rooms", "num_full_baths", "num_half_baths", "num_fireplaces",
    "type_of_residence", "construction_quality", "num_apartments", "attic_finish",
    "garage_attached", "garage_area_included", "garage_size", "garage_ext_wall_material",
    "attic_type", "basement_type", "ext_wall_material", "central_heating", "repair_condition",
    "basement_finish", "roof_material", "single_v_multi_family", "site_desirability",
    "num_commercial_units", "renovation", "recent_renovation", "porch", "central_air", "design_plan"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

-- selecting all records already in the full data_raw table
WITH records_in_data_raw_table AS (
    SELECT *, 1 AS retention_priority
    FROM {{ source('data_raw', 'cook_county_multifam_parcel_improvements') }}
),

-- selecting all distinct records from the latest data pull (in the "temp" table)
current_pull_with_distinct_combos_numbered AS (
    SELECT *,
        row_number() over(partition by
            {% for sc in source_cols %}{{ sc }},{% endfor %}
            {% for mc in metadata_cols %}{{ mc }}{{ "," if not loop.last }}{% endfor %}
        ) as rn
    FROM {{ source('data_raw', 'temp_cook_county_multifam_parcel_improvements') }}
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

{{ config(materialized='view') }}
{% set ck_cols = ["pin", "tax_year", "card_num"] %}
{% set record_id = "res_parcel_improvement_id" %}
{% set base_cols = [
    "res_parcel_improvement_id", "pin", "tax_year", "card_num", "class", "single_v_multi_family",
    "township_code", "cdu", "renovation", "recent_renovation", "pin_is_multicard", "pin_num_cards",
    "pin_is_multiland", "pin_num_landlines", "year_built", "building_sqft", "land_sqft",
    "num_bedrooms", "num_rooms", "num_full_baths", "num_half_baths", "num_fireplaces",
    "num_commercial_units", "num_apartments", "type_of_residence", "construction_quality",
    "garage_attached", "garage_area_included", "garage_size", "garage_ext_wall_material",
    "ext_wall_material", "roof_material", "attic_type", "attic_finish", "basement_type",
    "basement_finish", "repair_condition", "central_heating", "site_desirability", "porch",
    "central_air", "design_plan", "source_data_updated", "ingestion_check_time"
] %}

-- selects all records from the standardized view of this data
WITH std_data AS (
    SELECT *
    FROM {{ ref('cook_county_multifam_parcel_improvements_standardized') }}
),

-- keeps the most recently updated version of each record 
std_records_numbered_latest_first AS (
    SELECT *,
        row_number() over(partition by {{record_id}} ORDER BY source_data_updated DESC) as rn
    FROM std_data
),
most_current_records AS (
    SELECT *
    FROM std_records_numbered_latest_first
    WHERE rn = 1
),

-- selects the source_data_updated (ie the date of publication) value from each record's
--   first ingestion into the local data warehouse
std_records_numbered_earliest_first AS (
    SELECT *,
        row_number() over(partition by {{record_id}} ORDER BY source_data_updated ASC) as rn
FROM std_data
),
records_first_ingested_pub_date AS (
    SELECT {{record_id}}, source_data_updated AS first_ingested_pub_date
    FROM std_records_numbered_earliest_first
    WHERE rn = 1
)

SELECT
    {% for bc in base_cols %}mcr.{{ bc }},{% endfor %}
    fi.first_ingested_pub_date
FROM most_current_records AS mcr
LEFT JOIN records_first_ingested_pub_date AS fi
ON mcr.{{ record_id }} = fi.{{ record_id }}
ORDER BY {% for ck in ck_cols %}mcr.{{ ck }} DESC, {% endfor %} mcr.source_data_updated DESC

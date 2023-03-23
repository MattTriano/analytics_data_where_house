{{ config(materialized='view') }}
{% set ck_cols = ["rowid"] %}
{% set record_id = "rowid" %}
{% set base_cols = [
    "rowid", "appealid", "appealtrk", "appealseq", "pin", "pin_dash", "pin10", "township_code",
    "tax_year", "class", "majorclass", "result", "changereason", "changereasondescription",
    "nochangereason", "nochangereasondescription", "bor_landvalue", "bor_improvementvalue",
    "bor_totalvalue", "assessor_landvalue", "assessor_improvementvalue", "assessor_totalvalue",
    "taxcode", "vol", "appealtype", "appealtypedescription", "appellant", "appellant_address",
    "appellant_city", "appellant_zip", "appellant_state", "attny", "attorneycode",
    "attorney_firmname", "attorney_firstname", "attorney_lastname", "xcoord_crs_3435",
    "ycoord_crs_3435", "lat", "long", "geometry", "source_data_updated", "ingestion_check_time"
] %}

-- selects all records from the standardized view of this data
WITH std_data AS (
    SELECT *
    FROM {{ ref('cook_county_parcel_assessment_appeals_standardized') }}
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

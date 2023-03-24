{{ config(materialized='view') }}
{% set ck_cols = ["address", "lot", "sale_price", "block", "sale_date"] %}
{% set record_id = "nyc_parcel_sale_id" %}
{% set base_cols = [
    "nyc_parcel_sale_id", "sale_price", "sale_date", "block", "lot", "borough", "address",
    "apartment_number", "zip_code", "neighborhood", "building_class_category",
    "tax_class_at_time_of_sale", "building_class_at_time_of_sale", "tax_class_at_present",
    "building_class_at_present", "residential_units", "commercial_units", "total_units",
    "land_square_feet", "gross_square_feet", "year_built", "source_data_updated",
    "ingestion_check_time"
] %}

-- selects all records from the standardized view of this data
WITH std_data AS (
    SELECT *
    FROM {{ ref('nyc_parcel_sales_standardized') }}
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

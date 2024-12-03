{% set ck_cols = ["inventory_number", "tow_date", "plate"] %}
{% set record_id = "vehicle_tow_id" %}
{% set base_cols = [
    "vehicle_tow_id", "inventory_number", "tow_date", "make", "model", "style", "color", "plate",
    "state", "towed_to_address", "tow_facility_phone", "source_data_updated", "ingestion_check_time"
] %}

{{ config(
    materialized='incremental',
    unique_key=record_id,
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}
-- selects all records from the standardized view of this data
WITH std_data AS (
    SELECT *,
        md5(concat({% for col in base_cols %}
            coalesce({{ col }}::varchar, ''){% if not loop.last %}, '_', {% endif %}
        {% endfor %})) as record_hash
    FROM {{ ref('chicago_towed_vehicles_standardized') }}
    {% if is_incremental() %}
    WHERE {{record_id}} IN (
        -- Get IDs from new/updated records
        SELECT {{record_id}}
        FROM {{ ref('chicago_towed_vehicles_standardized') }}
        WHERE source_data_updated > (SELECT max(source_data_updated) FROM {{ this }})
    )
    {% endif %}
),

-- keeps the most recently updated version of each record
latest_records AS (
    SELECT *,
        row_number() over(partition by {{record_id}} ORDER BY source_data_updated DESC) as rn
    FROM std_data
),
new_or_updated_records AS (
    SELECT * FROM latest_records WHERE rn = 1
),

{% if is_incremental() %}
existing_versions AS (
    SELECT record_hash, min(source_data_updated) as version_first_seen
    FROM {{ this }}
    GROUP BY record_hash
),
{% endif %}

version_first_seen AS (
    {% if is_incremental() %}
    SELECT
        f.{{record_id}},
        COALESCE(e.version_first_seen, f.source_data_updated) as version_first_seen
    FROM new_or_updated_records f
    LEFT JOIN existing_versions e
    ON f.record_hash = e.record_hash
    {% else %}
    SELECT
        {{record_id}},
        source_data_updated as version_first_seen
    FROM new_or_updated_records
    {% endif %}
)

SELECT
    {% for bc in base_cols %}fnr.{{ bc }},{% endfor %}
    vfs.version_first_seen as first_ingested_pub_date,
    fnr.record_hash
FROM new_or_updated_records fnr
JOIN version_first_seen vfs
ON fnr.{{ record_id }} = vfs.{{ record_id }}
ORDER BY {% for ck in ck_cols %}fnr.{{ ck }} DESC, {% endfor %} fnr.source_data_updated DESC

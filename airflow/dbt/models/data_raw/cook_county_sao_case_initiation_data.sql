{{ config(materialized='table') }}
{% set source_cols = [
    "case_id", "case_participant_id", "received_date", "offense_category", "primary_charge_flag",
    "charge_id", "charge_version_id", "charge_offense_title", "charge_count", "chapter", "act",
    "section", "class", "aoic", "event", "event_date", "finding_no_probable_cause",
    "arraignment_date", "bond_date_initial", "bond_date_current", "bond_type_initial",
    "bond_type_current", "bond_amount_initial", "bond_amount_current",
    "bond_electronic_monitor_flag_initial", "bond_electroinic_monitor_flag_current",
    "age_at_incident", "race", "gender", "incident_city", "incident_begin_date",
    "incident_end_date", "law_enforcement_agency", "law_enforcement_unit", "arrest_date",
    "felony_review_date", "felony_review_result", "updated_offense_category"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

-- selecting all records already in the full data_raw table
WITH records_in_data_raw_table AS (
    SELECT *, 1 AS retention_priority
    FROM {{ source('data_raw', 'cook_county_sao_case_initiation_data') }}
),

-- selecting all distinct records from the latest data pull (in the "temp" table)
current_pull_with_distinct_combos_numbered AS (
    SELECT *,
        row_number() over(partition by
            {% for sc in source_cols %}{{ sc }},{% endfor %}
            {% for mc in metadata_cols %}{{ mc }}{{ "," if not loop.last }}{% endfor %}
        ) as rn
    FROM {{ source('data_raw', 'temp_cook_county_sao_case_initiation_data') }}
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

{{ config(materialized='view') }}
{% set ck_cols = ["case_participant_id", "diversion_program"] %}
{% set record_id = "participant_diversion_id" %}

WITH records_with_basic_cleaning AS (
    SELECT
        case_participant_id::bigint                           AS case_participant_id,
        upper(diversion_program::text)                        AS diversion_program,
        case_id::bigint                                       AS case_id,
        received_date::date                                   AS received_date,
        referral_date::date                                   AS referral_date,
        upper(offense_category::text)                         AS offense_category,        
        upper(primary_charge_offense_title::text)             AS primary_charge_offense_title,
        upper(statute::text)                                  AS statute,
        upper(race::text)                                     AS race,
        CASE
            WHEN upper(gender::text) IS NULL THEN NULL
            WHEN upper(gender::text) = 'FEMALE' THEN 'FEMALE'
            WHEN upper(gender::text) = 'MALE' THEN 'MALE'
            WHEN upper(gender::text) LIKE '%UNK%' THEN 'UNKNOWN'
            ELSE upper(gender::text)
        END                                                   AS gender,
        diversion_count::smallint                             AS diversion_count,
        upper(diversion_result::text)                         AS diversion_result,
        diversion_closed_date::date                           AS diversion_closed_date,
        source_data_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS source_data_updated,
        ingestion_check_time::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS ingestion_check_time
    FROM {{ ref('cook_county_sao_case_diversion_data') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated

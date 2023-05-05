{{ config(materialized='view') }}
{% set ck_cols = ["case_participant_id"] %}
{% set record_id = "case_participant_id" %}

WITH records_with_basic_cleaning AS (
    SELECT
        case_participant_id::bigint                               AS case_participant_id,
        case_id::bigint                                           AS case_id,
        received_date::date                                       AS received_date,
        incident_begin_date::date                                 AS incident_begin_date,
        incident_end_date::date                                   AS incident_end_date,
        arrest_date::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago'     AS arrest_date,
        upper(offense_category::text)                             AS offense_category,
        upper(participant_status::text)                           AS participant_status,
        age_at_incident::smallint                                 AS age_at_incident,
        upper(race::text)                                         AS race,        
        CASE
            WHEN upper(gender::text) IS NULL THEN NULL
            WHEN upper(gender::text) = 'FEMALE' THEN 'FEMALE'
            WHEN upper(gender::text) = 'MALE' THEN 'MALE'
            WHEN upper(gender::text) LIKE '%UNK%' THEN 'UNKNOWN'
            ELSE upper(gender::text)
        END                                                       AS gender,
        upper(incident_city::text)                                AS incident_city,        
        upper(law_enforcement_agency::text)                       AS law_enforcement_agency,
        upper(law_enforcement_unit::text)                         AS law_enforcement_unit,
        felony_review_date::date                                  AS felony_review_date,
        upper(felony_review_result::text)                         AS felony_review_result,
        upper(update_offense_category::text)                      AS update_offense_category,
        source_data_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago'     AS source_data_updated,
        ingestion_check_time::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago'     AS ingestion_check_time
    FROM {{ ref('cook_county_sao_case_intake_data') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated

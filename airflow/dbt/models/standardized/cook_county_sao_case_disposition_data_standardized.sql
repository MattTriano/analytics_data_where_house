{{ config(materialized='view') }}
{% set ck_cols = ["case_participant_id", "charge_id"] %}
{% set record_id = "charge_disposition_id" %}

WITH records_with_basic_cleaning AS (
    SELECT
        case_participant_id::bigint                           AS case_participant_id,
        charge_id::bigint                                     AS charge_id,
        charge_version_id::bigint                             AS charge_version_id,
        charge_count::smallint                                AS charge_count,
        case_id::bigint                                       AS case_id,
        received_date::date                                   AS received_date,
        upper(offense_category::text)                         AS offense_category,
        primary_charge_flag::boolean                          AS primary_charge_flag,
        upper(disposition_charged_offense_title::text)        AS disposition_charged_offense_title,
        disposition_date::date                                AS disposition_date,
        upper(disposition_charged_chapter::text)              AS disposition_charged_chapter,
        upper(disposition_charged_act::text)                  AS disposition_charged_act,
        upper(disposition_charged_section::text)              AS disposition_charged_section,
        upper(disposition_charged_class::text)                AS disposition_charged_class,
        upper(disposition_charged_aoic::text)                 AS disposition_charged_aoic,
        upper(charge_disposition::text)                       AS charge_disposition,
        upper(charge_disposition_reason::text)                AS charge_disposition_reason,
        upper(judge::text)                                    AS judge,
        upper(disposition_court_name::text)                   AS disposition_court_name,
        upper(disposition_court_facility::text)               AS disposition_court_facility,
        age_at_incident::smallint                             AS age_at_incident,
        upper(race::text)                                     AS race,
        CASE
            WHEN upper(gender::text) IS NULL THEN NULL
            WHEN upper(gender::text) = 'FEMALE' THEN 'FEMALE'
            WHEN upper(gender::text) = 'MALE' THEN 'MALE'
            WHEN upper(gender::text) LIKE '%UNK%' THEN 'UNKNOWN'
            ELSE upper(gender::text)
        END                                                   AS gender,
        upper(incident_city::text)                            AS incident_city,
        incident_begin_date::date                             AS incident_begin_date,
        incident_end_date::date                               AS incident_end_date,
        upper(law_enforcement_agency::text)                   AS law_enforcement_agency,
        upper(law_enforcement_unit::text)                     AS law_enforcement_unit,
        arrest_date::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS arrest_date,
        felony_review_date::date                              AS felony_review_date,
        upper(felony_review_result::text)                     AS felony_review_result,
        arraignment_date::date                                AS arraignment_date,
        upper(updated_offense_category::text)                 AS updated_offense_category,
        source_data_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS source_data_updated,
        ingestion_check_time::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS ingestion_check_time
    FROM {{ ref('cook_county_sao_case_disposition_data') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated

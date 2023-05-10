{{ config(materialized='view') }}
{% set ck_cols = ["case_participant_id", "charge_id"] %}
{% set record_id = "case_initiation_id" %}

WITH records_with_basic_cleaning AS (
    SELECT
        case_participant_id::bigint                           AS case_participant_id,
        case_id::bigint                                       AS case_id,
        charge_id::bigint                                     AS charge_id,
        charge_version_id::bigint                             AS charge_version_id,
        received_date::date                                   AS received_date,
        upper(offense_category::text)                         AS offense_category,
        upper(charge_offense_title::text)                     AS charge_offense_title,
        primary_charge_flag::boolean                          AS primary_charge_flag,
        charge_count::bigint                                  AS charge_count,
        upper(chapter::text)                                  AS chapter,
        upper(act::text)                                      AS act,
        upper(section::text)                                  AS section,
        upper(class::text)                                    AS class,
        upper(aoic::text)                                     AS aoic,
        upper(event::text)                                    AS event,
        event_date::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS event_date,
        arraignment_date::date                                AS arraignment_date,
        finding_no_probable_cause::int::boolean               AS finding_no_probable_cause,
        bond_date_initial::date                               AS bond_date_initial,
        bond_date_current::date                               AS bond_date_current,
        upper(bond_type_initial::text)                        AS bond_type_initial,
        upper(bond_type_current::text)                        AS bond_type_current,
        bond_amount_initial::bigint                           AS bond_amount_initial,
        bond_amount_current::bigint                           AS bond_amount_current,
        bond_electronic_monitor_flag_initial::boolean         AS bond_electronic_monitor_flag_initial,
        bond_electroinic_monitor_flag_current::boolean        AS bond_electroinic_monitor_flag_current,
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
        upper(updated_offense_category::text)                 AS updated_offense_category,
        source_data_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS source_data_updated,
        ingestion_check_time::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS ingestion_check_time
    FROM {{ ref('cook_county_sao_case_initiation_data') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated

{{ config(materialized='view') }}
{% set ck_cols = [
    "ticket_no", "case_id", "respondent", "comment", "address", "code_violation"
] %}
{% set record_id = "violation_notice_id" %}

WITH records_with_basic_cleaning AS (
    SELECT
        upper(ticket_no::text)                                AS ticket_no,
        upper(case_id::text)                                  AS case_id,
        upper(respondent::text)                               AS respondent,
        upper(comment::text)                                  AS comment,
        upper(address::text)                                  AS address,
        upper(code_violation::text)                           AS code_violation,
        violation_date::date                                  AS violation_date,
        upper(case_type::text)                                AS case_type,
        upper(disposition::text)                              AS disposition,
        fine_amount::money                                    AS fine_amount,
        upper(case_status::text)                              AS case_status,
        street_number::int                                    AS street_number,
        street_number_to::int                                 AS street_number_to,
        upper(direction::text)                                AS direction,
        upper(street_name::text)                              AS street_name,
        upper(street_type::text)                              AS street_type,
        upper(data_source::text)                              AS data_source,
        latitude::double precision                            AS latitude,
        longitude::double precision                           AS longitude,
        geometry::GEOMETRY(POINT,4326)                        AS geometry,
        source_data_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS source_data_updated,
        ingestion_check_time::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS ingestion_check_time
    FROM {{ ref('chicago_dph_environmental_enforcements') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated

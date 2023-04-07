{{ config(materialized='view') }}
{% set ck_cols = ["violation_number, "property_address", "entity_or_person_s_"] %}
{% set record_id = "vacant_bldg_violation_id" %}

WITH records_with_basic_cleaning AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
        upper(violation_number::text)                   AS violation_number,
        upper(property_address::text)                   AS property_address,
        upper(entity_or_person_s_::text)                AS entity_or_persons,
        issued_date::date                               AS issued_date,
        upper(violation_type::text)                     AS violation_type,
        upper(docket_number::text)                      AS docket_number,
        last_hearing_date::timestamp                    AS last_hearing_date,
        upper(issuing_department::text)                 AS issuing_department,
        upper(disposition_description::text)            AS disposition_description,
        total_fines::numeric(8,2)                       AS total_fines,
        interest_amount::numeric(8,2)                   AS interest_amount,
        total_administrative_costs::numeric(8,2)        AS total_administrative_costs,
        original_total_amount_due::numeric(8,2)         AS original_total_amount_due,
        collection_costs_or_attorney_fees::numeric(8,2) AS collection_costs_or_attorney_fees,
        court_cost::numeric(8,2)                        AS court_cost,
        total_paid::numeric(8,2)                        AS total_paid,
        current_amount_due::numeric(8,2)                AS current_amount_due,
        latitude::double precision                      AS latitude,
        longitude::double precision                     AS longitude,
        geometry::GEOMETRY(POINT, 4326)                 AS geometry,
        source_data_updated::timestamptz                AS source_data_updated,
        ingestion_check_time::timestamptz               AS ingestion_check_time
    FROM {{ ref('chicago_vacant_and_abandoned_buildings') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT *
FROM records_with_basic_cleaning
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated

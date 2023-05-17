{{ config(materialized='view') }}
{% set ck_cols = ["permit_number"] %}
{% set record_id = "permit_number" %}

WITH records_with_basic_cleaning AS (
    SELECT
        upper(permit_number::text)                            AS permit_number,
        upper(doing_business_as_name::text)                   AS doing_business_as_name,
        upper(legal_name::text)                               AS legal_name,
        upper(address::text)                                  AS address,
        account_number::int                                   AS account_number,
        site_number::smallint                                 AS site_number,
        issued_date::date                                     AS issued_date,
        expiration_date::date                                 AS expiration_date,
        payment_date::DATE                                    AS payment_date,
        upper(address_number_start::text)                     AS address_number_start,
        upper(address_number::text)                           AS address_number,
        upper(street_direction::text)                         AS street_direction,
        upper(street::text)                                   AS street,
        upper(street_type::text)                              AS street_type,
        upper(city::text)                                     AS city,
        upper(state::text)                                    AS state,
        upper(zip_code::text)                                 AS zip_code,
        lpad(upper(police_district::char(2)), 2, '0')         AS police_district,
        ward::smallint                                        AS ward,
        longitude::double precision                           AS longitude,
        latitude::double precision                            AS latitude,
        geometry::GEOMETRY(POINT, 4326)                       AS geometry,
        source_data_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS source_data_updated,
        ingestion_check_time::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS ingestion_check_time
    FROM {{ ref('chicago_sidewalk_cafe_permits') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated

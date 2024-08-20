{{ config(materialized='view') }}
{% set ck_cols = ["mural_registration_id"] %}
{% set record_id = "mural_registration_id" %}

WITH records_with_basic_cleaning AS (
    SELECT
        mural_registration_id::integer                        AS mural_registration_id,
        upper(artist_credit::text)                            AS artist_credit,
        upper(artwork_title::text)                            AS artwork_title,
        upper(description_of_artwork::text)                   AS description_of_artwork,
        upper(affiliated_or_commissioning::text)              AS affiliated_or_commissioning,
        upper(year_installed::text)                           AS year_installed,
        upper(year_restored::text)                            AS year_restored,
        upper(media::text)                                    AS media,
        upper(location_description::text)                     AS location_description,
        upper(street_address::text)                           AS street_address,
        upper(zip::text)                                      AS zip,
        upper(community_areas::text)                          AS community_areas,
        upper(ward::text)                                     AS ward,
        latitude::double precision                            AS latitude,
        longitude::double precision                           AS longitude,
        geometry::GEOMETRY(POINT,4326)                        AS geometry,
        source_data_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS source_data_updated,
        ingestion_check_time::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS ingestion_check_time
    FROM {{ ref('chicago_murals') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated

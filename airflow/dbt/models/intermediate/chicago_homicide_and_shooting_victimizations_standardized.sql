{{ config(materialized='view') }}
{% set ck_cols = ["unique_id"] %}
{% set record_id = "unique_id" %}

WITH records_with_basic_cleaning AS (
    SELECT
        upper(unique_id::text)                    AS unique_id,
        upper(case_number::text)                  AS case_number,
        date::timestamp                           AS date,
        lpad(hour::char(2), 2, '0')               AS hour,
        day_of_week::smallint                     AS day_of_week,
        lpad(month::char(2), 2, '0')              AS month,
        updated::timestamp                        AS updated,
        upper(block::text)                        AS block,
        upper(zip_code::char(5))                  AS zip_code,
        upper(community_area::text)               AS community_area,
        lpad(upper(beat::char(4)), 4, '0')        AS beat,
        lpad(upper(district::char(2)), 2, '0')    AS district,
        upper(area::char(1))                      AS area,
        ward::smallint                            AS ward,
        upper(state_house_district::varchar(3))   AS state_house_district,
        upper(state_senate_district::varchar(2))  AS state_senate_district,
        gunshot_injury_i::boolean                 AS gunshot_injury_i,
        upper(incident_primary::text)             AS incident_primary,
        upper(incident_iucr_secondary::text)      AS incident_iucr_secondary,
        upper(incident_iucr_cd::varchar(4))       AS incident_iucr_cd,
        upper(incident_fbi_cd::varchar(3))        AS incident_fbi_cd,
        upper(incident_fbi_descr::text)           AS incident_fbi_descr,
        upper(victimization_primary::text)        AS victimization_primary,
        upper(victimization_iucr_secondary::text) AS victimization_iucr_secondary,
        upper(victimization_iucr_cd::varchar(4))  AS victimization_iucr_cd,
        upper(victimization_fbi_cd::varchar(3))   AS victimization_fbi_cd,
        upper(victimization_fbi_descr::text)      AS victimization_fbi_descr,
        upper(location_description::text)         AS location_description,
        upper(age::text)                          AS age,
        upper(sex::text)                          AS sex,
        upper(race::text)                         AS race,
        upper(homicide_victim_first_name::text)   AS homicide_victim_first_name,
        upper(homicide_victim_mi::text)           AS homicide_victim_mi,
        upper(homicide_victim_last_name::text)    AS homicide_victim_last_name,
        upper(street_outreach_organization::text) AS street_outreach_organization,
        latitude::double precision                AS latitude,
        longitude::double precision               AS longitude,
        geometry::GEOMETRY(GEOMETRY, 4326)        AS geometry,
        source_data_updated::timestamptz          AS source_data_updated,
        ingestion_check_time::timestamptz         AS ingestion_check_time
    FROM {{ ref('chicago_homicide_and_shooting_victimizations') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT *
FROM records_with_basic_cleaning
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated

{{ config(materialized='view') }}

WITH neighborhood_boundary_data AS
(
  SELECT *, row_number() over(partition by town_nbhd) as rn
  FROM {{ source('staging','cook_county_neighborhoods') }}
)

SELECT
    {{ dbt_utils.surrogate_key(['town_nbhd']) }} AS nbhd_id,
    upper(triad_name::text) AS triad_name,
    lpad(town_nbhd::int::varchar(5), 5, '0') AS town_nbhd,
    lpad(township_code::int::varchar(2), 2, '0') AS township_code,
    triad_code::char(1) AS triad_code,
    upper(township_name::text) AS township_name,
    lpad(nbhd::int::varchar(3), 3, '0') AS nbhd,
    geometry::GEOMETRY(MULTIPOLYGON, 4326) AS geometry,
    ingested_on::text AS ingested_on
FROM neighborhood_boundary_data
WHERE rn = 1

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}

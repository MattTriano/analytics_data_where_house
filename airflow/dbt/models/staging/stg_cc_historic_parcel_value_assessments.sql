{{ config(materialized='view') }}

WITH assessment_data AS
(
  SELECT *, row_number() over(partition by pin, tax_year) as rn
  FROM {{ source('staging','temp_cook_county_parcel_value_assessments') }}
)

SELECT
    {{ dbt_utils.surrogate_key(['pin', 'tax_year']) }} as assessment_id,
    pin::bigint AS pin,
    tax_year::int AS tax_year,
    upper(class::varchar(6)) AS class,
    lpad(township_code::int::varchar(2), 2, '0')  AS township_code,
    upper(township_name::text) AS township_name,
	mailed_bldg::bigint AS mailed_bldg,
	mailed_land::bigint AS mailed_land,
	mailed_tot::bigint AS mailed_tot,
	certified_bldg::bigint AS certified_bldg,
	certified_land::bigint AS certified_land,
	certified_tot::bigint AS certified_tot ,
	board_bldg::bigint AS board_bldg,
	board_land::bigint AS board_land,
	board_tot::bigint AS board_tot
FROM assessment_data
WHERE rn = 1

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}

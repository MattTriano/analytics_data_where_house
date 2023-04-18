{{ config(materialized='view') }}
{% set ck_cols = ["pin", "tax_year", "card_num"] %}
{% set record_id = "res_parcel_improvement_id" %}

WITH records_with_basic_cleaning AS (
    SELECT
        pin::bigint                                     AS pin,
        tax_year::bigint                                AS tax_year,
        card_num::bigint                                AS card_num,
        upper(class::varchar(3))                        AS class,
        upper(single_v_multi_family::text)              AS single_v_multi_family,
        upper(township_code::varchar(2))                AS township_code,
        upper(cdu::char(2))                             AS cdu,
        renovation::boolean                             AS renovation,
        recent_renovation::boolean                      AS recent_renovation,
        pin_is_multicard::boolean                       AS pin_is_multicard,
        pin_num_cards::smallint                         AS pin_num_cards,
        pin_is_multiland::boolean                       AS pin_is_multiland,
        pin_num_landlines::smallint                     AS pin_num_landlines,
        year_built::smallint                            AS year_built,
        building_sqft::int                              AS building_sqft,
        land_sqft::int                                  AS land_sqft,
        num_bedrooms::smallint                          AS num_bedrooms,
        num_rooms::smallint                             AS num_rooms,
        num_full_baths::smallint                        AS num_full_baths,
        num_half_baths::smallint                        AS num_half_baths,
        num_fireplaces::smallint                        AS num_fireplaces,
        num_commercial_units::smallint                  AS num_commercial_units,
        upper(num_apartments::text)                     AS num_apartments,
        upper(type_of_residence::text)                  AS type_of_residence,
        upper(construction_quality::text)               AS construction_quality,
        garage_attached::boolean                        AS garage_attached,
        garage_area_included::boolean                   AS garage_area_included,
        upper(garage_size::text)                        AS garage_size,
        upper(garage_ext_wall_material::text)           AS garage_ext_wall_material,
        upper(ext_wall_material::text)                  AS ext_wall_material,
        upper(roof_material::text)                      AS roof_material,
        upper(attic_type::text)                         AS attic_type,
        upper(attic_finish::text)                       AS attic_finish,
        upper(basement_type::text)                      AS basement_type,
        upper(basement_finish::text)                    AS basement_finish,
        upper(repair_condition::text)                   AS repair_condition,
        upper(central_heating::text)                    AS central_heating,
        upper(site_desirability::text)                  AS site_desirability,
        upper(porch::text)                              AS porch,
        upper(central_air::text)                        AS central_air,
        upper(design_plan::text)                        AS design_plan,
        source_data_updated::timestamptz                AS source_data_updated,
        ingestion_check_time::timestamptz               AS ingestion_check_time
    FROM {{ ref('cook_county_multifam_parcel_improvements') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated

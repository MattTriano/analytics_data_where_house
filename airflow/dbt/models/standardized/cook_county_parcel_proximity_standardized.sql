{{ config(materialized='view') }}
{% set ck_cols = ["pin", "year"] %}
{% set record_id = "pin_year_id" %}

WITH records_with_basic_cleaning AS (
    SELECT
        pin10::bigint                                         AS pin,
        year::bigint                                          AS year,
        num_pin_in_half_mile::bigint                          AS num_pin_in_half_mile,
        num_bus_stop_in_half_mile::double precision           AS num_bus_stop_in_half_mile,
        num_bus_stop_data_year::double precision              AS num_bus_stop_data_year,
        num_foreclosure_in_half_mile_past_5_years::bigint     AS num_foreclosure_in_half_mile_past_5_years,
        num_foreclosure_per_1000_pin_past_5_years::double precision AS num_foreclosure_per_1000_pin_past_5_years,
        upper(num_foreclosure_data_year::text)                AS num_foreclosure_data_year,
        num_school_in_half_mile::double precision             AS num_school_in_half_mile,
        num_school_data_year::double precision                AS num_school_data_year,
        airport_dnl_total::double precision                   AS airport_dnl_total,
        upper(nearest_bike_trail_id::text)                    AS nearest_bike_trail_id,
        upper(nearest_bike_trail_name::text)                  AS nearest_bike_trail_name,
        nearest_bike_trail_dist_ft::double precision          AS nearest_bike_trail_dist_ft,
        nearest_bike_trail_data_year::double precision        AS nearest_bike_trail_data_year,
        nearest_cemetery_gnis_code::double precision          AS nearest_cemetery_gnis_code,
        upper(nearest_cemetery_name::text)                    AS nearest_cemetery_name,
        nearest_cemetery_dist_ft::double precision            AS nearest_cemetery_dist_ft,
        nearest_cemetery_data_year::double precision          AS nearest_cemetery_data_year,
        upper(nearest_cta_route_id::text)                     AS nearest_cta_route_id,
        upper(nearest_cta_route_name::text)                   AS nearest_cta_route_name,
        nearest_cta_route_dist_ft::double precision           AS nearest_cta_route_dist_ft,
        nearest_cta_route_data_year::double precision         AS nearest_cta_route_data_year,
        nearest_cta_stop_id::double precision                 AS nearest_cta_stop_id,
        upper(nearest_cta_stop_name::text)                    AS nearest_cta_stop_name,
        nearest_cta_stop_dist_ft::double precision            AS nearest_cta_stop_dist_ft,
        nearest_cta_stop_data_year::double precision          AS nearest_cta_stop_data_year,
        upper(nearest_golf_course_id::text)                   AS nearest_golf_course_id,
        nearest_golf_course_dist_ft::double precision         AS nearest_golf_course_dist_ft,
        nearest_golf_course_data_year::double precision       AS nearest_golf_course_data_year,
        nearest_hospital_gnis_code::double precision          AS nearest_hospital_gnis_code,
        upper(nearest_hospital_name::text)                    AS nearest_hospital_name,
        nearest_hospital_dist_ft::double precision            AS nearest_hospital_dist_ft,
        nearest_hospital_data_year::double precision          AS nearest_hospital_data_year,
        lake_michigan_dist_ft::double precision               AS lake_michigan_dist_ft,
        lake_michigan_data_year::double precision             AS lake_michigan_data_year,
        nearest_major_road_osm_id::double precision           AS nearest_major_road_osm_id,
        upper(nearest_major_road_name::text)                  AS nearest_major_road_name,
        nearest_major_road_dist_ft::double precision          AS nearest_major_road_dist_ft,
        nearest_major_road_data_year::double precision        AS nearest_major_road_data_year,
        upper(nearest_metra_route_id::text)                   AS nearest_metra_route_id,
        upper(nearest_metra_route_name::text)                 AS nearest_metra_route_name,
        nearest_metra_route_dist_ft::double precision         AS nearest_metra_route_dist_ft,
        nearest_metra_route_data_year::double precision       AS nearest_metra_route_data_year,
        upper(nearest_metra_stop_id::text)                    AS nearest_metra_stop_id,
        upper(nearest_metra_stop_name::text)                  AS nearest_metra_stop_name,
        nearest_metra_stop_dist_ft::double precision          AS nearest_metra_stop_dist_ft,
        nearest_metra_stop_data_year::double precision        AS nearest_metra_stop_data_year,
        nearest_park_osm_id::double precision                 AS nearest_park_osm_id,
        upper(nearest_park_name::text)                        AS nearest_park_name,
        nearest_park_dist_ft::double precision                AS nearest_park_dist_ft,
        nearest_park_data_year::double precision              AS nearest_park_data_year,
        nearest_railroad_id::double precision                 AS nearest_railroad_id,
        upper(nearest_railroad_name::text)                    AS nearest_railroad_name,
        nearest_railroad_dist_ft::double precision            AS nearest_railroad_dist_ft,
        nearest_railroad_data_year::double precision          AS nearest_railroad_data_year,
        nearest_secondary_road_osm_id::double precision       AS nearest_secondary_road_osm_id,
        upper(nearest_secondary_road_name::text)              AS nearest_secondary_road_name,
        nearest_secondary_road_dist_ft::double precision      AS nearest_secondary_road_dist_ft,
        nearest_secondary_road_data_year::double precision    AS nearest_secondary_road_data_year,
        nearest_water_id::double precision                    AS nearest_water_id,
        upper(nearest_water_name::text)                       AS nearest_water_name,
        nearest_water_dist_ft::double precision               AS nearest_water_dist_ft,
        nearest_water_data_year::double precision             AS nearest_water_data_year,
        nearest_neighbor_1_pin10::bigint                      AS nearest_neighbor_1_pin10,
        nearest_neighbor_1_dist_ft::double precision          AS nearest_neighbor_1_dist_ft,
        nearest_neighbor_2_pin10::bigint                      AS nearest_neighbor_2_pin10,
        nearest_neighbor_2_dist_ft::double precision          AS nearest_neighbor_2_dist_ft,
        nearest_neighbor_3_pin10::bigint                      AS nearest_neighbor_3_pin10,
        nearest_neighbor_3_dist_ft::double precision          AS nearest_neighbor_3_dist_ft,
        source_data_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS source_data_updated,
        ingestion_check_time::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS ingestion_check_time
    FROM {{ ref('cook_county_parcel_proximity') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated

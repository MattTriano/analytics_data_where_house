{% set dataset_name = "cook_county_parcel_proximity" %}
{% set ck_cols = ["pin", "year"] %}
{% set record_id = "pin_year_id" %}
{% set base_cols = [
    "pin_year_id", "pin", "year", "num_pin_in_half_mile", "num_bus_stop_in_half_mile",
    "num_bus_stop_data_year", "num_foreclosure_in_half_mile_past_5_years",
    "num_foreclosure_per_1000_pin_past_5_years", "num_foreclosure_data_year",
    "num_school_in_half_mile", "num_school_data_year", "airport_dnl_total", "nearest_bike_trail_id",
    "nearest_bike_trail_name", "nearest_bike_trail_dist_ft", "nearest_bike_trail_data_year",
    "nearest_cemetery_gnis_code", "nearest_cemetery_name", "nearest_cemetery_dist_ft",
    "nearest_cemetery_data_year", "nearest_cta_route_id", "nearest_cta_route_name",
    "nearest_cta_route_dist_ft", "nearest_cta_route_data_year", "nearest_cta_stop_id",
    "nearest_cta_stop_name", "nearest_cta_stop_dist_ft", "nearest_cta_stop_data_year",
    "nearest_golf_course_id", "nearest_golf_course_dist_ft", "nearest_golf_course_data_year",
    "nearest_hospital_gnis_code", "nearest_hospital_name", "nearest_hospital_dist_ft",
    "nearest_hospital_data_year", "lake_michigan_dist_ft", "lake_michigan_data_year",
    "nearest_major_road_osm_id", "nearest_major_road_name", "nearest_major_road_dist_ft",
    "nearest_major_road_data_year", "nearest_metra_route_id", "nearest_metra_route_name",
    "nearest_metra_route_dist_ft", "nearest_metra_route_data_year", "nearest_metra_stop_id",
    "nearest_metra_stop_name", "nearest_metra_stop_dist_ft", "nearest_metra_stop_data_year",
    "nearest_park_osm_id", "nearest_park_name", "nearest_park_dist_ft", "nearest_park_data_year",
    "nearest_railroad_id", "nearest_railroad_name", "nearest_railroad_dist_ft",
    "nearest_railroad_data_year", "nearest_secondary_road_osm_id", "nearest_secondary_road_name",
    "nearest_secondary_road_dist_ft", "nearest_secondary_road_data_year", "nearest_water_id",
    "nearest_water_name", "nearest_water_dist_ft", "nearest_water_data_year",
    "nearest_neighbor_1_pin10", "nearest_neighbor_1_dist_ft", "nearest_neighbor_2_pin10",
    "nearest_neighbor_2_dist_ft", "nearest_neighbor_3_pin10", "nearest_neighbor_3_dist_ft",
    "source_data_updated", "ingestion_check_time"
] %}
{% set updated_at_col = "source_data_updated" %}

{% set query = generate_clean_stage_incremental_dedupe_query(
    dataset_name=dataset_name,
    record_id=record_id,
    ck_cols=ck_cols,
    base_cols=base_cols,
    updated_at_col=updated_at_col
) %}

{{ query }}

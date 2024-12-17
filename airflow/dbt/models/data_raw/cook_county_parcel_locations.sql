{% set dataset_name = "cook_county_parcel_locations" %}
{% set source_cols = [
     "pin", "property_address", "property_apt_no", "property_city", "property_zip",
     "mailing_address", "mailing_state", "mailing_city", "mailing_zip", "longitude", "latitude",
     "township", "township_name", "nbhd", "tract_geoid", "tract_pop", "tract_white_perc",
     "tract_black_perc", "tract_asian_perc", "tract_his_perc", "tract_other_perc",
     "tract_midincome", "puma", "municipality_fips", "municipality", "commissioner_dist",
     "reps_dist", "senate_dist", "ward", "ssa_name", "ssa_no", "tif_agencynum", "ohare_noise",
     "floodplain", "fs_flood_factor", "fs_flood_risk_direction", "withinmr100", "withinmr101300",
     "school_elem_district", "school_hs_district", "indicator_has_address", "indicator_has_latlon"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}

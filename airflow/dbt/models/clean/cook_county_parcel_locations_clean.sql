{% set dataset_name = "cook_county_parcel_locations" %}
{% set ck_cols = ["pin"] %}
{% set record_id = "pin" %}
{% set base_cols = [
    "pin", "property_address", "property_apt_no", "property_city", "property_zip", "nbhd",
    "township", "township_name", "municipality", "municipality_fips", "ward", "puma", "tract_geoid",
    "tract_pop", "tract_white_perc", "tract_black_perc", "tract_asian_perc", "tract_his_perc",
    "tract_other_perc", "tract_midincome", "commissioner_dist", "reps_dist", "senate_dist",
    "ssa_name", "ssa_no", "tif_agencynum", "school_elem_district", "school_hs_district",
    "mailing_address", "mailing_city", "mailing_zip", "mailing_state", "ohare_noise", "floodplain",
    "fs_flood_factor", "fs_flood_risk_direction", "withinmr100", "withinmr101300",
    "indicator_has_address", "indicator_has_latlon", "longitude", "latitude", "source_data_updated",
    "ingestion_check_time"
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

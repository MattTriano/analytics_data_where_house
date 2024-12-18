{% set dataset_name = "cc_planning_db_housing_and_demos_by_bg" %}
{% set source_cols = [
    "state", "county", "tract", "block_group", "gidbg", "renter_occp_hu_cen_2010",
    "renter_occp_hu_acs_14_18", "owner_occp_hu_acs_14_18", "single_unit_acs_14_18",
    "avg_tot_prns_in_hhd_cen_2010", "tot_housing_units_cen_2010", "tot_vacant_units_cen_2010",
    "tot_occp_units_cen_2010", "avg_tot_prns_in_hhd_acs_14_18", "tot_housing_units_acs_14_18",
    "tot_vacant_units_acs_14_18", "tot_occp_units_acs_14_18", "mlt_u2_9_strc_acs_14_18",
    "mlt_u10p_acs_14_18", "no_plumb_acs_14_18", "recent_built_hu_acs_14_18",
    "tot_population_acs_14_18", "median_age_acs_14_18", "pop_65plus_acs_14_18",
    "pop_5_17_acs_14_18", "pop_18_24_acs_14_18", "pop_25_44_acs_14_18", "pop_45_64_acs_14_18",
    "females_acs_14_18", "males_acs_14_18", "avg_agg_hh_inc_acs_14_18",
    "aggregate_hh_inc_acs_14_18", "med_hhd_inc_bg_acs_14_18", "pub_asst_inc_acs_14_18",
    "diff_hu_1yr_ago_acs_14_18", "hhd_moved_in_acs_14_18", "hhd_ppl_und_18_acs_14_18",
    "female_no_hb_cen_2010", "rel_family_hhd_cen_2010", "nonfamily_hhd_acs_14_18",
    "rel_family_hhd_acs_14_18", "mrdcple_fmly_hhd_acs_14_18", "not_mrdcple_hhd_acs_14_18",
    "female_no_hb_acs_14_18", "sngl_prns_hhd_acs_14_18", "dataset_base_url", "dataset_id"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}

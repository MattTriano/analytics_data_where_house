{% set dataset_name = "chicago_street_center_lines" %}
{% set source_cols = [
     "ewns_dir", "l_parity", "oneway_dir", "logiclf", "street_nam", "street_typ", "r_t_add",
     "l_censusbl", "r_parity", "r_zip", "edit_type", "tiered", "tnode_id", "edit_date",
     "create_tim", "suf_dir", "logicrf", "t_cross", "update_tim", "ewns", "objectid", "l_fips",
     "pre_dir", "logicrt", "status_dat", "f_zlev", "l_f_add", "r_f_add", "streetname",
     "flag_strin", "dir_travel", "ewns_coord", "status", "l_zip", "t_cross_st", "f_cross",
     "l_t_add", "t_zlev", "class", "length", "f_cross_st", "fnode_id", "create_use", "logiclt",
     "r_fips", "r_censusbl", "update_use", "shape_len", "trans_id", "geometry"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}

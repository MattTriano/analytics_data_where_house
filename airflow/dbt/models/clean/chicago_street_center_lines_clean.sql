{% set dataset_name = "chicago_street_center_lines" %}
{% set ck_cols = ["objectid"] %}
{% set record_id = "objectid" %}
{% set base_cols = [
    "objectid", "pre_dir", "street_name", "street_type", "suffix_dir", "oneway_dir", "dir_travel",
    "tiered", "left_to_addr", "left_from_add", "left_parity", "left_zip", "left_fips",
    "left_censusbl", "right_from_add", "right_to_addr", "right_parity", "right_zip", "right_fips",
    "right_censusbl", "to_zlev", "from_cross", "from_cross_st", "to_cross", "to_cross_st",
    "logic_left_from", "logic_left_to", "logic_right_from", "logic_right_to", "ewns", "ewns_dir",
    "ewns_coord", "class", "street_id", "from_node_id", "to_node_id", "trans_id", "flag_string",
    "status", "status_date", "create_use", "create_time", "edit_type", "edit_date", "update_use",
    "update_time", "length", "shape_len", "geometry", "source_data_updated", "ingestion_check_time"
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

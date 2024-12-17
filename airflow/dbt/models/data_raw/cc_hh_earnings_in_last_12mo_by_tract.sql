{% set dataset_name = "cc_hh_earnings_in_last_12mo_by_tract" %}
{% set source_cols = [
    "b19051_001e", "b19051_001ea", "b19051_001m", "b19051_001ma", "b19051_002e", "b19051_002ea",
    "b19051_002m", "b19051_002ma", "b19051_003e", "b19051_003ea", "b19051_003m", "b19051_003ma",
    "geo_id", "name", "state", "county", "tract", "dataset_base_url", "dataset_id"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}

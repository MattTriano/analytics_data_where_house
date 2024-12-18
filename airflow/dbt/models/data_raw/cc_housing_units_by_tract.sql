{% set dataset_name = "cc_housing_units_by_tract" %}
{% set source_cols = [
    "b25001_001e", "b25001_001ea", "b25001_001m", "b25001_001ma", "geo_id", "name", "state",
    "county", "tract", "dataset_base_url", "dataset_id"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}

{% set dataset_name = "cc_housing_occupancy_by_tract_acs5" %}
{% set source_cols = [
    "geo_id", "name", "b25001_001e", "b25001_001ea", "b25001_001m", "b25001_001ma", "b25002_001e",
    "b25002_001ea", "b25002_001m", "b25002_001ma", "b25002_002e", "b25002_002ea", "b25002_002m",
    "b25002_002ma", "b25002_003e", "b25002_003ea", "b25002_003m", "b25002_003ma", "b25003_001e",
    "b25003_001ea", "b25003_001m", "b25003_001ma", "b25003_002e", "b25003_002ea", "b25003_002m",
    "b25003_002ma", "b25003_003e", "b25003_003ea", "b25003_003m", "b25003_003ma", "state", "county",
    "tract", "dataset_base_url", "dataset_id"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}


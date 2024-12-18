{% set dataset_name = "chicago_shotspotter_alerts" %}
{% set source_cols = [
    "zip_code", "area", "illinois_senate_district", "street_outreach_organization", "latitude",
    "day_of_week", "unique_id", "longitude", "hour", "rounds", "block", "ward",
    "incident_type_description", "date", "beat", "community_area", "district",
    "illinois_house_district", "month", "geometry"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}

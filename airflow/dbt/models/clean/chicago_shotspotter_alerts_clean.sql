{% set dataset_name = "chicago_shotspotter_alerts" %}
{% set ck_cols = ["unique_id"] %}
{% set record_id = "unique_id" %}
{% set base_cols = [
    "unique_id", "date", "hour", "day_of_week", "month", "incident_type_description", "rounds",
    "block", "zip_code", "community_area", "beat", "district", "area", "ward",
    "illinois_house_district", "illinois_senate_district", "street_outreach_organization",
    "latitude", "longitude", "geometry", "source_data_updated", "ingestion_check_time"
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

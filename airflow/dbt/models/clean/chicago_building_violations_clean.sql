{% set dataset_name = "chicago_building_violations" %}
{% set ck_cols = ["id"] %}
{% set record_id = "id" %}
{% set base_cols = [
    "id", "violation_date", "violation_code", "violation_description", "violation_status",
    "violation_status_date", "violation_location", "violation_ordinance",
    "violation_last_modified_date", "inspection_number", "inspector_id",
    "violation_inspector_comments", "inspection_waived", "inspection_category", "inspection_status",
    "department_bureau", "property_group", "ssa", "address", "street_number", "street_direction",
    "street_name", "street_type", "longitude", "latitude", "geometry", "source_data_updated",
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

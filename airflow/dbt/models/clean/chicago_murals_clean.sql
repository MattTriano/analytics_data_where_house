{% set dataset_name = "chicago_murals" %}
{% set ck_cols = ["mural_registration_id"] %}
{% set record_id = "mural_registration_id" %}
{% set base_cols = [
    "mural_registration_id", "artist_credit", "artwork_title", "description_of_artwork",
    "affiliated_or_commissioning", "year_installed", "year_restored", "media",
    "location_description", "street_address", "zip", "community_areas", "ward", "latitude",
    "longitude", "geometry", "source_data_updated", "ingestion_check_time"
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

{% set dataset_name = "chicago_murals" %}
{% set source_cols = [
    "artist_credit", "description_of_artwork", "artwork_title", "affiliated_or_commissioning",
    "latitude", "zip", "longitude", "community_areas", "mural_registration_id", "ward", "media",
    "location_description", "year_installed", "street_address", "year_restored", "geometry"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}

{% set dataset_name = "illinois_census_tracts_2022" %}
{% set source_cols = [
    "statefp", "countyfp", "tractce", "geoid", "name", "namelsad", "mtfcc", "funcstat", "aland",
    "awater", "intptlat", "intptlon", "geometry", "vintage_year"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}

{% set dataset_name = "united_states_rails_2022" %}
{% set ck_cols = ["linearid"] %}
{% set record_id = "linearid" %}
{% set base_cols = [
    "linearid", "vintage_year", "fullname", "mtfcc", "geometry", "source_data_updated",
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

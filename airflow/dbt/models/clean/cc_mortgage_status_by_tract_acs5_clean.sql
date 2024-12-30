{% set dataset_name = "cc_mortgage_status_by_tract_acs5" %}
{% set ck_cols = ["geo_id"] %}
{% set record_id = "geo_id" %}
{% set base_cols = [
    "geo_id", "name", "b25081_001e", "b25081_001ea", "b25081_001m", "b25081_001ma", "b25081_002e",
    "b25081_002ea", "b25081_002m", "b25081_002ma", "b25081_003e", "b25081_003ea", "b25081_003m",
    "b25081_003ma", "b25081_004e", "b25081_004ea", "b25081_004m", "b25081_004ma", "b25081_005e",
    "b25081_005ea", "b25081_005m", "b25081_005ma", "b25081_006e", "b25081_006ea", "b25081_006m",
    "b25081_006ma", "b25081_007e", "b25081_007ea", "b25081_007m", "b25081_007ma", "b25081_008e",
    "b25081_008ea", "b25081_008m", "b25081_008ma", "b25081_009e", "b25081_009ea", "b25081_009m",
    "b25081_009ma", "state", "county", "tract", "dataset_base_url", "dataset_id",
    "source_data_updated", "ingestion_check_time"
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

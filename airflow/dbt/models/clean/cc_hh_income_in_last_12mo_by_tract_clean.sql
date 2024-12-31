{% set dataset_name = "cc_hh_income_in_last_12mo_by_tract" %}
{% set ck_cols = ["geo_id"] %}
{% set record_id = "geo_id" %}
{% set base_cols = [
    "geo_id", "name", "b19001_001e", "b19001_001ea", "b19001_001m", "b19001_001ma", "b19001_002e",
    "b19001_002ea", "b19001_002m", "b19001_002ma", "b19001_003e", "b19001_003ea", "b19001_003m",
    "b19001_003ma", "b19001_004e", "b19001_004ea", "b19001_004m", "b19001_004ma", "b19001_005e",
    "b19001_005ea", "b19001_005m", "b19001_005ma", "b19001_006e", "b19001_006ea", "b19001_006m",
    "b19001_006ma", "b19001_007e", "b19001_007ea", "b19001_007m", "b19001_007ma", "b19001_008e",
    "b19001_008ea", "b19001_008m", "b19001_008ma", "b19001_009e", "b19001_009ea", "b19001_009m",
    "b19001_009ma", "b19001_010e", "b19001_010ea", "b19001_010m", "b19001_010ma", "b19001_011e",
    "b19001_011ea", "b19001_011m", "b19001_011ma", "b19001_012e", "b19001_012ea", "b19001_012m",
    "b19001_012ma", "b19001_013e", "b19001_013ea", "b19001_013m", "b19001_013ma", "b19001_014e",
    "b19001_014ea", "b19001_014m", "b19001_014ma", "b19001_015e", "b19001_015ea", "b19001_015m",
    "b19001_015ma", "b19001_016e", "b19001_016ea", "b19001_016m", "b19001_016ma", "b19001_017e",
    "b19001_017ea", "b19001_017m", "b19001_017ma", "state", "county", "tract", "dataset_base_url",
    "dataset_id", "source_data_updated", "ingestion_check_time"
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

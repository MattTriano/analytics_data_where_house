{% set dataset_name = "chicago_homicide_and_shooting_victimizations" %}
{% set ck_cols = ["unique_id"] %}
{% set record_id = "unique_id" %}
{% set base_cols = [
    "unique_id", "case_number", "date", "hour", "day_of_week", "month", "updated", "block",
    "zip_code", "community_area", "beat", "district", "area", "ward", "state_house_district",
    "state_senate_district", "gunshot_injury_i", "incident_primary", "incident_iucr_secondary",
    "incident_iucr_cd", "incident_fbi_cd", "incident_fbi_descr", "victimization_primary",
    "victimization_iucr_secondary", "victimization_iucr_cd", "victimization_fbi_cd",
    "victimization_fbi_descr", "location_description", "age", "sex", "race",
    "homicide_victim_first_name", "homicide_victim_mi", "homicide_victim_last_name",
    "street_outreach_organization", "latitude", "longitude", "geometry", "source_data_updated",
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

{% set dataset_name = "chicago_parks" %}
{% set ck_cols = ["park_no"] %}
{% set record_id = "park_no" %}
{% set base_cols = [
    "park_no", "park", "label", "location", "zip", "park_class", "wheelchr_accs", "archery_ranges",
    "artificial_turf", "baseball_fields", "baseball_jr_fields", "batting_cages", "basketball",
    "basketba_1", "bocce_courts", "bowling_gr", "boxing_gym", "chess_tables", "climbing_walls",
    "cricket_fields", "croquet", "fitness_courses", "fitness_center", "football_fields",
    "golf_courses", "golf_putting", "golf_driving", "minigolf", "gymnasium", "gymnastic_centers",
    "handball_r", "handball_i", "horseshoe_courts", "iceskating", "mountain_biking", "playground",
    "playgrou_1", "roller_court", "shuffleboard", "skate_park", "sled_hill", "tennis_courts",
    "track", "volleyball", "volleyba_1", "beach", "boat_launches", "boat_lau_1", "boat_slips",
    "fishing", "rowing_club", "harbor", "modelyacht_basin", "pool_indoor", "pool_outdoor",
    "spray_feat", "water_slid", "water_play", "nature_birds", "conservato", "lagoon", "garden",
    "nature_cen", "wetland_area", "zoo", "band_shell", "cultural_center", "gallery", "modeltrain",
    "carousel", "community_center", "dog_friend", "senior_cen", "gisobjid", "objectid_1",
    "shape_area", "perimeter", "shape_leng", "acres", "ward", "geometry", "source_data_updated",
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

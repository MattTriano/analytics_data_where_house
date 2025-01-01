{% set dataset_name = "chicago_parks" %}
{% set source_cols = [
    "pool_outdo", "golf_cours", "baseball_b", "wheelchr_a", "acres", "bocce_cour", "sled_hill",
    "artificial", "shape_area", "location", "tennis_cou", "perimeter", "zip", "gymnasium",
    "horseshoe_", "objectid_1", "boat_lau_1", "harbor", "nature_bir", "spray_feat", "game_table",
    "golf_drivi", "fitness_co", "cricket_fi", "boat_slips", "track", "boxing_cen", "golf_putti",
    "label", "handball_r", "community_", "modeltrain", "mountain_b", "carousel", "senior_cen",
    "fitness_ce", "lagoon", "football_s", "garden", "baseball_j", "wetland_ar", "archery_ra",
    "gallery", "shape_leng", "conservato", "rowing_clu", "gymnastic_", "playground", "ward",
    "handball_i", "basketball", "shuffleboa", "band_shell", "cultural_c", "croquet", "bowling_gr",
    "playgrou_1", "water_slid", "gisobjid", "park", "nature_cen", "modelyacht", "pool_indoo",
    "skate_park", "casting_pi", "sport_roll", "baseball_s", "water_play", "dog_friend",
    "basketba_1", "park_class", "volleyba_1", "iceskating", "park_no", "climbing_w", "boat_launc",
    "beach", "volleyball", "zoo", "minigolf", "geometry"
] %}
{% set metadata_cols = ["source_data_updated", "ingestion_check_time"] %}

{% set query = get_and_add_new_and_updated_records_to_data_raw(
    dataset_name=dataset_name,
    source_cols=source_cols,
    metadata_cols=metadata_cols
) %}

{{- query -}}


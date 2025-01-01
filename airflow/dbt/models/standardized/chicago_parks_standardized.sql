{{ config(materialized='view') }}
{% set ck_cols = ["park_no"] %}
{% set record_id = "park_no" %}

WITH records_with_basic_cleaning AS (
    SELECT
        park_no::numeric::bigint               AS park_no,
        upper(park::text)                      AS park,
        upper(label::text)                     AS label,
        upper(location::text)                  AS location,
        upper(zip::text)                       AS zip,
        upper(park_class::text)                AS park_class,
        wheelchr_a::smallint                   AS wheelchr_accs,

        -- sport, play, wellness
        archery_ra::smallint                   AS archery_ranges,
        artificial::smallint                   AS artificial_turf,
        baseball_s::smallint                   AS baseball_fields,
        baseball_j::smallint                   AS baseball_jr_fields,
        baseball_b::smallint                   AS batting_cages,
        basketball::smallint                   AS basketball,
        basketba_1::smallint                   AS basketba_1,
        bocce_cour::smallint                   AS bocce_courts,
        bowling_gr::smallint                   AS bowling_gr,
        boxing_cen::smallint                   AS boxing_gym,
        game_table::smallint                   AS chess_tables,
        climbing_w::smallint                   AS climbing_walls,
        cricket_fi::smallint                   AS cricket_fields,
        croquet::smallint                      AS croquet,
        fitness_co::smallint                   AS fitness_courses,
        fitness_ce::smallint                   AS fitness_center,
        football_s::smallint                   AS football_fields,
        golf_cours::smallint                   AS golf_courses,
        golf_putti::smallint                   AS golf_putting,
        golf_drivi::smallint                   AS golf_driving,
        minigolf::smallint                     AS minigolf,
        gymnasium::smallint                    AS gymnasium,
        gymnastic_::smallint                   AS gymnastic_centers,
        handball_r::smallint                   AS handball_r,
        handball_i::smallint                   AS handball_i,
        horseshoe_::smallint                   AS horseshoe_courts,
        iceskating::smallint                   AS iceskating,
        mountain_b::smallint                   AS mountain_biking,
        playground::smallint                   AS playground,
        playgrou_1::smallint                   AS playgrou_1,
        sport_roll::smallint                   AS roller_court,
        shuffleboa::smallint                   AS shuffleboard,
        skate_park::smallint                   AS skate_park,
        sled_hill::smallint                    AS sled_hill,
        tennis_cou::smallint                   AS tennis_courts,
        track::smallint                        AS track,
        volleyball::smallint                   AS volleyball,
        volleyba_1::smallint                   AS volleyba_1,

        -- water
        beach::smallint                        AS beach,
        boat_launc::smallint                   AS boat_launches,
        boat_lau_1::smallint                   AS boat_lau_1,
        boat_slips::smallint                   AS boat_slips,
        casting_pi::smallint                   AS fishing,
        rowing_clu::smallint                   AS rowing_club,
        harbor::smallint                       AS harbor,
        modelyacht::smallint                   AS modelyacht_basin,
        pool_indoo::smallint                   AS pool_indoor,
        pool_outdo::smallint                   AS pool_outdoor,
        spray_feat::smallint                   AS spray_feat,
        water_slid::smallint                   AS water_slid,
        water_play::smallint                   AS water_play,

        -- nature
        nature_bir::smallint                   AS nature_birds,
        conservato::smallint                   AS conservato,
        lagoon::smallint                       AS lagoon,
        garden::smallint                       AS garden,
        nature_cen::smallint                   AS nature_cen,
        wetland_ar::smallint                   AS wetland_area,
        zoo::smallint                          AS zoo,

        -- arts and culture
        band_shell::smallint                   AS band_shell,
        cultural_c::smallint                   AS cultural_center,
        gallery::smallint                      AS gallery,
        modeltrain::smallint                   AS modeltrain,

        -- amenities
        carousel::smallint                     AS carousel,
        community_::smallint                   AS community_center,
        dog_friend::smallint                   AS dog_friend,
        senior_cen::smallint                   AS senior_cen,

        -- geometry
        gisobjid::numeric::bigint              AS gisobjid,
        objectid_1::bigint                     AS objectid_1,
        shape_area::numeric                    AS shape_area,
        perimeter::numeric                     AS perimeter,
        shape_leng::numeric                    AS shape_leng,
        acres::numeric                         AS acres,
        ward::numeric::smallint                AS ward,
        geometry::GEOMETRY(MULTIPOLYGON, 4326) AS geometry,
        source_data_updated::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS source_data_updated,
        ingestion_check_time::timestamptz
            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago' AS ingestion_check_time
    FROM {{ ref('chicago_parks') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated

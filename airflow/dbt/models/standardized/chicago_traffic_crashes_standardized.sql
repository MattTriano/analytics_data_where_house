{{ config(materialized='view') }}
{% set ck_cols = ["crash_record_id"] %}
{% set record_id = "crash_record_id" %}

WITH records_with_basic_cleaning AS (
    SELECT
        upper(crash_record_id::text)            AS crash_record_id,
        crash_date::timestamp                   AS crash_date,
        street_no::int                          AS street_no,
        upper(street_direction::char(1))        AS street_direction,
        upper(street_name::text)                AS street_name,
        crash_month::smallint                   AS crash_month,
        crash_day_of_week::smallint             AS crash_day_of_week,
        crash_hour::smallint                    AS crash_hour,
        date_police_notified::timestamp         AS date_police_notified,
        injuries_fatal::smallint                AS injuries_fatal,
        injuries_non_incapacitating::smallint   AS injuries_non_incapacitating,
        injuries_incapacitating::smallint       AS injuries_incapacitating,
        injuries_no_indication::smallint        AS injuries_no_indication,
        injuries_unknown::smallint              AS injuries_unknown,
        injuries_reported_not_evident::smallint AS injuries_reported_not_evident,
        injuries_total::smallint                AS injuries_total,
        upper(most_severe_injury::text)         AS most_severe_injury,
        upper(damage::text)                     AS damage,
        num_units::smallint                     AS num_units,
        upper(work_zone_type::text)             AS work_zone_type,
        upper(workers_present_i::char(1))       AS workers_present_i,
        upper(crash_date_est_i::char(1))        AS crash_date_est_i,
        upper(photos_taken_i::char(1))          AS photos_taken_i,
        upper(private_property_i::char(1))      AS private_property_i,
        upper(intersection_related_i::char(1))  AS intersection_related_i,
        upper(statements_taken_i::char(1))      AS statements_taken_i,
        upper(hit_and_run_i::char(1))           AS hit_and_run_i,
        upper(work_zone_i::char(1))             AS work_zone_i,
        upper(dooring_i::char(1))               AS dooring_i,
        lane_cnt::int                           AS lane_cnt,
        upper(trafficway_type::text)            AS trafficway_type,
        upper(traffic_control_device::text)     AS traffic_control_device,
        upper(road_defect::text)                AS road_defect,
        upper(lighting_condition::text)         AS lighting_condition,
        upper(device_condition::text)           AS device_condition,
        upper(weather_condition::text)          AS weather_condition,
        upper(alignment::text)                  AS alignment,
        upper(roadway_surface_cond::text)       AS roadway_surface_cond,
        posted_speed_limit::smallint            AS posted_speed_limit,
        upper(first_crash_type::text)           AS first_crash_type,
        upper(prim_contributory_cause::text)    AS prim_contributory_cause,
        upper(sec_contributory_cause::text)     AS sec_contributory_cause,
        upper(rd_no::text)                      AS rd_no,
        beat_of_occurrence::smallint            AS beat_of_occurrence,
        upper(report_type::text)                AS report_type,
        upper(crash_type::text)                 AS crash_type,
        latitude::double precision              AS latitude,
        longitude::double precision             AS longitude,
        geometry::GEOMETRY(GEOMETRY,4326)       AS geometry,
        source_data_updated::timestamptz        AS source_data_updated,
        ingestion_check_time::timestamptz       AS ingestion_check_time
    FROM {{ ref('chicago_traffic_crashes') }}
    ORDER BY {% for ck in ck_cols %}{{ ck }}{{ "," if not loop.last }}{% endfor %}
)


SELECT
    {% if ck_cols|length > 1 %}
        {{ dbt_utils.generate_surrogate_key(ck_cols) }} AS {{ record_id }},
    {% endif %}
    a.*
FROM records_with_basic_cleaning AS a
ORDER BY {% for ck in ck_cols %}{{ ck }},{% endfor %} source_data_updated

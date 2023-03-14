WITH pin_attrs AS (
  SELECT
    pin,
    chicago_community,
    cpd_beat,
    cpd_district,
    property_address,
    ward,
    tract_geoid,
    ohare_noise,
    floodplain,
    fs_flood_factor,
    fs_flood_risk_direction,
    withinmr100,
    withinmr101300
  FROM {{ ref('cook_county_parcel_locations_dim') }}
  WHERE chicago_community IS NOT NULL
),
nbhd_geoms AS (
  SELECT
    community,
    json_build_object(
      'type', 'Polygon', 'geometry',
      ST_AsGeoJSON(ST_Transform(
        (ST_DUMP(geometry)).geom::geometry(Polygon, 4326), 4326)
      )::json)::text AS chi_nbhd_boundary
  FROM {{ ref('chicago_community_area_boundaries_clean') }} 
),
pin_nbhd_geoms AS (
  SELECT
    p.pin,
    p.chicago_community,
    p.cpd_beat,
    p.cpd_district,
    p.property_address,
    p.ward,
    p.tract_geoid,
    p.ohare_noise,
    p.floodplain,
    p.fs_flood_factor,
    p.fs_flood_risk_direction,
    p.withinmr100,
    p.withinmr101300,
    g.chi_nbhd_boundary
  FROM pin_attrs AS p
  LEFT JOIN nbhd_geoms AS g
  ON p.chicago_community = g.community 
),
pin_sales AS (
    SELECT
        parcel_sale_id,
        pin,
        class,
        class_descr,
        is_multisale,
        num_parcels_sale,
        sale_price,
        sale_date,
        last_sale_price,
        last_sale_date,
        price_change_since_last_sale,
        years_since_last_sale,
        last_sale_was_multisale,
        num_parcels_last_sale,
        sale_document_num,
        is_mydec_date,
        sale_deed_type,
        sale_type,
        township_code,
        sale_seller_name,
        sale_buyer_name
    FROM {{ ref('cook_county_parcel_sales_fact') }} 
)

SELECT
    p.pin,
    p.chicago_community,
    p.cpd_beat,
    p.cpd_district,
    p.property_address,
    p.ward,
    p.tract_geoid,
    p.ohare_noise,
    p.floodplain,
    p.fs_flood_factor,
    p.fs_flood_risk_direction,
    p.withinmr100,
    p.withinmr101300,
    s.parcel_sale_id,
    s.class,
    s.class_descr,
    s.is_multisale,
    s.num_parcels_sale,
    s.sale_price,
    s.sale_date,
    s.last_sale_price,
    s.last_sale_date,
    s.price_change_since_last_sale,
    s.years_since_last_sale,
    s.last_sale_was_multisale,
    s.num_parcels_last_sale,
    s.sale_document_num,
    s.is_mydec_date,
    s.sale_deed_type,
    s.sale_type,
    s.township_code,
    s.sale_seller_name,
    s.sale_buyer_name,
    p.chi_nbhd_boundary
FROM pin_nbhd_geoms AS p
INNER JOIN pin_sales AS s
ON p.pin = s.pin

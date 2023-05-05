from cc_utils.socrata import SocrataTable

CHICAGO_311_SERVICE_REQUESTS = SocrataTable(
    table_id="v6vf-nfxy",
    table_name="chicago_311_service_requests",
    schedule="52 3 * * 4",
)

CHICAGO_AFFORDABLE_RENTAL_HOUSING = SocrataTable(
    table_id="s6ha-ppgi",
    table_name="chicago_affordable_rental_housing",
    schedule="40 2 * * 1",
)

CHICAGO_BIKE_PATHS = SocrataTable(
    table_id="3w5d-sru8",
    table_name="chicago_bike_paths",
    schedule="0 22 10 8 *",
)

CHICAGO_BUSINESS_LICENSES = SocrataTable(
    table_id="r5kz-chrr",
    table_name="chicago_business_licenses",
    schedule="51 4 * * 3",
)

CHICAGO_CITY_BOUNDARY = SocrataTable(
    table_id="ewy2-6yfk",
    table_name="chicago_city_boundary",
    schedule="0 20 10 8 *",
)

CHICAGO_COMMUNITY_AREA_BOUNDARIES = SocrataTable(
    table_id="cauq-8yn6",
    table_name="chicago_community_area_boundaries",
    schedule="41 5 3 9 *",
)

CHICAGO_CRIMES = SocrataTable(
    table_id="ijzp-q8t2",
    table_name="chicago_crimes",
    schedule="10 4 * * *",
)

CHICAGO_CTA_TRAIN_LINES = SocrataTable(
    table_id="53r7-y88m",
    table_name="chicago_cta_train_lines",
    schedule="12 5 5 * *",
)

CHICAGO_CTA_TRAIN_STATIONS = SocrataTable(
    table_id="8pix-ypme",
    table_name="chicago_cta_train_stations",
    schedule="0 5 5 * *",
)

CHICAGO_CTA_BUS_STOPS = SocrataTable(
    table_id="hvnx-qtky",
    table_name="chicago_cta_bus_stops",
    schedule="30 5 5 * *",
)

CHICAGO_DIVVY_STATIONS = SocrataTable(
    table_id="bbyy-e7gq",
    table_name="chicago_divvy_stations",
    schedule="30 5 * * 1",
)

CHICAGO_FOOD_INSPECTIONS = SocrataTable(
    table_id="4ijn-s7e5",
    table_name="chicago_food_inspections",
    schedule="30 8,20 * * *",
)

CHICAGO_HOMICIDES_AND_SHOOTING_VICTIMIZATIONS = SocrataTable(
    table_id="gumc-mgzr",
    table_name="chicago_homicide_and_shooting_victimizations",
    schedule="0 7,19 * * *",
)

CHICAGO_MURALS = SocrataTable(
    table_id="we8h-apcf",
    table_name="chicago_murals",
    schedule="47 6 * * *",
)

CHICAGO_POLICE_BEAT_BOUNDARIES = SocrataTable(
    table_id="aerh-rz74",
    table_name="chicago_police_beat_boundaries",
    schedule="47 7 3 * *",
)

CHICAGO_POLICE_DISTRICT_BOUNDARIES = SocrataTable(
    table_id="fthy-xz3r",
    table_name="chicago_police_district_boundaries",
    schedule="47 6 3 * *",
)

CHICAGO_POLICE_STATIONS = SocrataTable(
    table_id="z8bn-74gv",
    table_name="chicago_police_stations",
    schedule="47 5 3 * *",
)

CHICAGO_POTHOLES_PATCHED = SocrataTable(
    table_id="wqdh-9gek",
    table_name="chicago_potholes_patched",
    schedule="47 8 * * *",
)

CHICAGO_RED_LIGHT_CAMERA_VIOLATIONS = SocrataTable(
    table_id="spqx-js37",
    table_name="chicago_red_light_camera_violations",
    schedule="35 05 * * *",
)

CHICAGO_RELOCATED_VEHICLES = SocrataTable(
    table_id="5k2z-suxx",
    table_name="chicago_relocated_vehicles",
    schedule="35 11,23 * * *",
)

CHICAGO_SIDEWALK_CAFE_PERMITS = SocrataTable(
    table_id="nxj5-ix6z",
    table_name="chicago_sidewalk_cafe_permits",
    schedule="50 3 * * *",
)

CHICAGO_SHOTSPOTTER_ALERTS = SocrataTable(
    table_id="3h7q-7mdb",
    table_name="chicago_shotspotter_alerts",
    schedule="20 6,18 * * *",
)

CHICAGO_STREET_CENTER_LINES = SocrataTable(
    table_id="6imu-meau",
    table_name="chicago_street_center_lines",
    schedule="20 0 2 2 *",
)

CHICAGO_TOWED_VEHICLES = SocrataTable(
    table_id="ygr5-vcbg",
    table_name="chicago_towed_vehicles",
    schedule="15 11,23 * * *",
)

CHICAGO_TRAFFIC_CRASHES_LINES = SocrataTable(
    table_id="85ca-t3if",
    table_name="chicago_traffic_crashes",
    schedule="20 3,15 * * *",
)

CHICAGO_VACANT_AND_ABANDONED_BUILDINGS = SocrataTable(
    table_id="kc9i-wq85",
    table_name="chicago_vacant_and_abandoned_buildings",
    schedule="21 15 * * *",
)

COOK_COUNTY_ADDRESS_POINTS = SocrataTable(
    table_id="78yw-iddh",
    table_name="cook_county_address_points",
    schedule="40 5 * * 2",
)

COOK_COUNTY_PARCEL_ASSESSMENT_APPEALS = SocrataTable(
    table_id="7pny-nedm",
    table_name="cook_county_parcel_assessment_appeals",
    schedule="0 13 * * 6",
)

COOK_COUNTY_NEIGHBORHOOD_BOUNDARIES = SocrataTable(
    table_id="wyzt-dzf8",
    table_name="cook_county_neighborhood_boundaries",
    schedule="0 4 3 3 *",
)

COOK_COUNTY_PARCEL_LOCATIONS = SocrataTable(
    table_id="c49d-89sn",
    table_name="cook_county_parcel_locations",
    schedule="0 7 4 * *",
)

COOK_COUNTY_PARCEL_SALES = SocrataTable(
    table_id="wvhk-k5uv",
    table_name="cook_county_parcel_sales",
    schedule="0 6 4 * *",
)

COOK_COUNTY_PARCEL_VALUE_ASSESSMENTS = SocrataTable(
    table_id="uzyt-m557",
    table_name="cook_county_parcel_value_assessments",
    schedule="0 3 4 * *",
)

COOK_COUNTY_MULTIFAM_PARCEL_IMPROVEMENTS = SocrataTable(
    table_id="x54s-btds",
    table_name="cook_county_multifam_parcel_improvements",
    schedule="20 3 * * 4",
)

COOK_COUNTY_CONDO_PARCEL_IMPROVEMENTS = SocrataTable(
    table_id="3r7i-mrz4",
    table_name="cook_county_condo_parcel_improvements",
    schedule="20 3 * * 3",
)

COOK_COUNTY_SAO_CASE_INTAKE_DATA = SocrataTable(
    table_id="3k7z-hchi", table_name="cook_county_sao_case_intake_data", schedule="11 1 11 * *"
)

COOK_COUNTY_SAO_CASE_INITIATION_DATA = SocrataTable(
    table_id="7mck-ehwz", table_name="cook_county_sao_case_initiation_data", schedule="31 1 11 * *"
)

COOK_COUNTY_SAO_CASE_DISPOSITION_DATA = SocrataTable(
    table_id="apwk-dzx8", table_name="cook_county_sao_case_disposition_data", schedule="51 1 11 * *"
)

COOK_COUNTY_SAO_CASE_DIVERSION_DATA = SocrataTable(
    table_id="gpu3-5dfh", table_name="cook_county_sao_case_diversion_data", schedule="11 2 11 * *"
)

COOK_COUNTY_SAO_CASE_SENTENCING_DATA = SocrataTable(
    table_id="tg8v-tm6u", table_name="cook_county_sao_case_sentencing_data", schedule="31 2 11 * *"
)

NYC_PARCEL_SALES = SocrataTable(
    table_id="usep-8jbt",
    table_name="nyc_parcel_sales",
    schedule="20 2 * * 3",
)

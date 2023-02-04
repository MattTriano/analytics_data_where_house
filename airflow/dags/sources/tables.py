from cc_utils.socrata import SocrataTable

CHICAGO_AFFORDABLE_RENTAL_HOUSING = SocrataTable(
    table_id="s6ha-ppgi",
    table_name="chicago_affordable_rental_housing",
    schedule="40 2 * * 1",
    clean_schedule="50 2 * * 1",
)

CHICAGO_CITY_BOUNDARY = SocrataTable(
    table_id="ewy2-6yfk",
    table_name="chicago_city_boundary",
    schedule="0 20 10 8 *",
    clean_schedule="10 20 10 8 *",
)

CHICAGO_CTA_TRAIN_STATIONS = SocrataTable(
    table_id="8pix-ypme",
    table_name="chicago_cta_train_stations",
    schedule="0 5 5 * *",
    clean_schedule="10 5 5 * *",
)

CHICAGO_CTA_BUS_STOPS = SocrataTable(
    table_id="hvnx-qtky",
    table_name="chicago_cta_bus_stops",
    schedule="30 5 5 * *",
    clean_schedule="45 5 5 * *",
)

CHICAGO_FOOD_INSPECTIONS = SocrataTable(
    table_id="4ijn-s7e5",
    table_name="chicago_food_inspections",
    schedule="30 8,20 * * *",
    clean_schedule="45 8,20 * * *",
)

CHICAGO_HOMICIDES_AND_SHOOTING_VICTIMIZATIONS = SocrataTable(
    table_id="gumc-mgzr",
    table_name="chicago_homicide_and_shooting_victimizations",
    schedule="0 7,19 * * *",
    clean_schedule="10 7,19 * * *",
)

CHICAGO_POTHOLES_PATCHED = SocrataTable(
    table_id="wqdh-9gek",
    table_name="chicago_potholes_patched",
    schedule="47 8 * * *",
    clean_schedule="52 8 * * *",
)

CHICAGO_RELOCATED_VEHICLES = SocrataTable(
    table_id="5k2z-suxx",
    table_name="chicago_relocated_vehicles",
    schedule="35 11,23 * * *",
    clean_schedule="43 11,23 * * *",
)

CHICAGO_SHOTSPOTTER_ALERTS = SocrataTable(
    table_id="3h7q-7mdb",
    table_name="chicago_shotspotter_alerts",
    schedule="20 6,18 * * *",
    clean_schedule="30 6,18 * * *",
)

CHICAGO_STREET_CENTER_LINES = SocrataTable(
    table_id="6imu-meau",
    table_name="chicago_street_center_lines",
    schedule="20 0 2 2 *",
    clean_schedule="40 0 2 2 *",
)

CHICAGO_TOWED_VEHICLES = SocrataTable(
    table_id="ygr5-vcbg",
    table_name="chicago_towed_vehicles",
    schedule="15 11,23 * * *",
    clean_schedule="23 11,23 * * *",
)

CHICAGO_TRAFFIC_CRASHES_LINES = SocrataTable(
    table_id="85ca-t3if",
    table_name="chicago_traffic_crashes",
    schedule="20 3,15 * * *",
    clean_schedule="40 3,15 * * *",
)

COOK_COUNTY_ADDRESS_POINTS = SocrataTable(
    table_id="78yw-iddh",
    table_name="cook_county_address_points",
    schedule="40 5 * * 2",
    clean_schedule="55 5 * * 2",
)

COOK_COUNTY_PARCEL_ASSESSMENT_APPEALS = SocrataTable(
    table_id="7pny-nedm",
    table_name="cook_county_parcel_assessment_appeals",
    schedule="0 13 * * 6",
    clean_schedule="30 14 * * 6",
)

COOK_COUNTY_NEIGHBORHOOD_BOUNDARIES = SocrataTable(
    table_id="wyzt-dzf8",
    table_name="cook_county_neighborhood_boundaries",
    schedule="0 4 3 3 *",
    clean_schedule="30 4 3 3 *",
)

COOK_COUNTY_PARCEL_LOCATIONS = SocrataTable(
    table_id="c49d-89sn",
    table_name="cook_county_parcel_locations",
    schedule="0 7 4 * *",
    clean_schedule="30 7 4 * *",
)

COOK_COUNTY_PARCEL_SALES = SocrataTable(
    table_id="wvhk-k5uv",
    table_name="cook_county_parcel_sales",
    schedule="0 6 4 * *",
    clean_schedule="30 6 4 * *",
)

COOK_COUNTY_PARCEL_VALUE_ASSESSMENTS = SocrataTable(
    table_id="uzyt-m557",
    table_name="cook_county_parcel_value_assessments",
    schedule="0 3 4 * *",
    clean_schedule="30 4 4 * *",
)

COOK_COUNTY_MULTIFAM_PARCEL_IMPROVEMENTS = SocrataTable(
    table_id="x54s-btds",
    table_name="cook_county_multifam_parcel_improvements",
    schedule="20 3 * * 4",
    clean_schedule="20 4 * * 4",
)

COOK_COUNTY_CONDO_PARCEL_IMPROVEMENTS = SocrataTable(
    table_id="3r7i-mrz4",
    table_name="cook_county_condo_parcel_improvements",
    schedule="20 3 * * 3",
    clean_schedule="20 4 * * 3",
)

from cc_utils.census.tiger import TIGERDataset
from sources.geographies import (
    COOK_COUNTY_CENSUS_TRACTS,
    ENTIRE_UNITED_STATES,
    ILLINOIS_CENSUS_TRACTS,
    UNITED_STATES_COUNTIES,
)

COOK_COUNTY_AREAWATERS_2022 = TIGERDataset(
    base_dataset_name="cook_county_areawaters",
    vintage_year=2022,
    entity_name="AREAWATER",
    geography=COOK_COUNTY_CENSUS_TRACTS,
    schedule="30 3 2 */3 *",
)

COOK_COUNTY_LINEAR_WATER_2024 = TIGERDataset(
    base_dataset_name="cook_county_linear_water",
    vintage_year=2024,
    entity_name="LINEARWATER",
    geography=COOK_COUNTY_CENSUS_TRACTS,
    schedule="45 4 3 */3 *",
)

COOK_COUNTY_ROADS_2022 = TIGERDataset(
    base_dataset_name="cook_county_roads",
    vintage_year=2022,
    entity_name="ROADS",
    geography=COOK_COUNTY_CENSUS_TRACTS,
    schedule="25 3 2 */3 *",
)

ILLINOIS_CENSUS_TRACTS_2022 = TIGERDataset(
    base_dataset_name="illinois_census_tracts",
    vintage_year=2022,
    entity_name="TRACT",
    geography=ILLINOIS_CENSUS_TRACTS,
    schedule="5 4 3 */3 *",
)

UNITED_STATES_RAILS_2022 = TIGERDataset(
    base_dataset_name="united_states_rails",
    vintage_year=2022,
    entity_name="RAILS",
    geography=ENTIRE_UNITED_STATES,
    schedule="15 4 3 */3 *",
)

UNITED_STATES_COUNTIES_2022 = TIGERDataset(
    base_dataset_name="united_states_counties",
    vintage_year=2022,
    entity_name="COUNTY",
    geography=UNITED_STATES_COUNTIES,
    schedule="25 4 3 */3 *",
)

UNITED_STATES_COUNTIES_2024 = TIGERDataset(
    base_dataset_name="united_states_counties",
    vintage_year=2024,
    entity_name="COUNTY",
    geography=UNITED_STATES_COUNTIES,
    schedule="35 4 3 */3 *",
)

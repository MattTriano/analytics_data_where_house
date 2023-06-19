from cc_utils.census.tiger import TIGERDataset
from sources.geographies import ILLINOIS_CENSUS_TRACTS, ENTIRE_UNITED_STATES

ILLINOIS_CENSUS_TRACTS_2022 = TIGERDataset(
    base_dataset_name="illinois_census_tracts",
    vintage_year=2022,
    entity_name="TRACT",
    geography=ILLINOIS_CENSUS_TRACTS,
    schedule="5 4 3 * *",
)

UNITED_STATES_RAILS_2022 = TIGERDataset(
    base_dataset_name="united_states_rails",
    vintage_year=2022,
    entity_name="RAILS",
    geography=ENTIRE_UNITED_STATES,
    schedule="15 4 3 * *",
)

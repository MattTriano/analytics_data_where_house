from cc_utils.census.api import (
    CensusGeogUS,
    CensusGeogCounty,
    CensusGeogTract,
    CensusGeogBlockGroup,
)

COOK_COUNTY_CENSUS_BLOCK_GROUPS = CensusGeogBlockGroup(state_cd="17", county_cd="031")
COOK_COUNTY_CENSUS_TRACTS = CensusGeogTract(state_cd="17", county_cd="031")

MIDWEST_CENSUS_TRACTS = CensusGeogTract(
    state_cd=["17", "18", "19", "20", "26", "27", "29", "31", "37", "39", "46", "55"]
)
ILLINOIS_CENSUS_TRACTS = CensusGeogTract(state_cd="17")
ILLINOIS_COUNTIES = CensusGeogCounty(state_cd="17", county_cd="*")
ENTIRE_UNITED_STATES = CensusGeogUS()
UNITED_STATES_COUNTIES = CensusGeogCounty(state_cd="*", county_cd="*")

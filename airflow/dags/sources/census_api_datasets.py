from cc_utils.census.api import (
    CensusGeogTract,
    CensusVariableGroupAPICall,
    CensusVariableGroupDataset,
)


GROSS_RENT_BY_ILLINOIS_TRACT = CensusVariableGroupDataset(
    dataset_name="gross_rent_by_illinois_tract",
    api_call_obj=CensusVariableGroupAPICall(
        dataset_base_url="http://api.census.gov/data/2021/acs/acs5",
        group_name="B25063",
        geographies=CensusGeogTract(state_cd="17"),
    ),
    schedule="0 2 10 4,10 *",
)

GROSS_RENT_BY_COOK_COUNTY_IL_TRACT = CensusVariableGroupDataset(
    dataset_name="gross_rent_by_cook_county_il_tract",
    api_call_obj=CensusVariableGroupAPICall(
        dataset_base_url="http://api.census.gov/data/2021/acs/acs5",
        group_name="B25063",
        geographies=CensusGeogTract(state_cd="17", county_cd="031"),
    ),
    schedule="5 2 10 4,10 *",
)

CC_HOUSING_UNITS_BY_TRACT = CensusVariableGroupDataset(
    dataset_name="cc_housing_units_by_tract",
    api_call_obj=CensusVariableGroupAPICall(
        dataset_base_url="http://api.census.gov/data/2021/acs/acs5",
        group_name="B25001",
        geographies=CensusGeogTract(state_cd="17", county_cd="031"),
    ),
    schedule="40 5 2 3,9 *",
)

CC_HH_EARNINGS_IN_LAST_12MO_BY_TRACT = CensusVariableGroupDataset(
    dataset_name="cc_hh_earnings_in_last_12mo_by_tract",
    api_call_obj=CensusVariableGroupAPICall(
        dataset_base_url="http://api.census.gov/data/2021/acs/acs5",
        group_name="B19051",
        geographies=CensusGeogTract(state_cd="17", county_cd="031"),
    ),
    schedule="40 5 20 3,9 *",
)

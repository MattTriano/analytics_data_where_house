from sources.geographies import (
    COOK_COUNTY_CENSUS_BLOCK_GROUPS,
    COOK_COUNTY_CENSUS_TRACTS,
    ILLINOIS_CENSUS_TRACTS,
)
from cc_utils.census.api import (
    CensusGeogTract,
    CensusGeogBlockGroup,
    CensusVariableGroupAPICall,
    CensusVariableGroupDataset,
    CensusDatasetVariablesAPICaller,
)


GROSS_RENT_BY_ILLINOIS_TRACT = CensusVariableGroupDataset(
    dataset_name="gross_rent_by_illinois_tract",
    api_call_obj=CensusVariableGroupAPICall(
        dataset_base_url="http://api.census.gov/data/2021/acs/acs5",
        group_name="B25063",
        geographies=ILLINOIS_CENSUS_TRACTS,
    ),
    schedule="0 2 10 4,10 *",
)

GROSS_RENT_BY_COOK_COUNTY_IL_TRACT = CensusVariableGroupDataset(
    dataset_name="gross_rent_by_cook_county_il_tract",
    api_call_obj=CensusVariableGroupAPICall(
        dataset_base_url="http://api.census.gov/data/2021/acs/acs5",
        group_name="B25063",
        geographies=COOK_COUNTY_CENSUS_TRACTS,
    ),
    schedule="5 2 10 4,10 *",
)

CC_HOUSING_UNITS_BY_TRACT = CensusVariableGroupDataset(
    dataset_name="cc_housing_units_by_tract",
    api_call_obj=CensusVariableGroupAPICall(
        dataset_base_url="http://api.census.gov/data/2021/acs/acs5",
        group_name="B25001",
        geographies=COOK_COUNTY_CENSUS_TRACTS,
    ),
    schedule="40 5 2 3,9 *",
)

CC_HH_EARNINGS_IN_LAST_12MO_BY_TRACT = CensusVariableGroupDataset(
    dataset_name="cc_hh_earnings_in_last_12mo_by_tract",
    api_call_obj=CensusVariableGroupAPICall(
        dataset_base_url="http://api.census.gov/data/2021/acs/acs5",
        group_name="B19051",
        geographies=COOK_COUNTY_CENSUS_TRACTS,
    ),
    schedule="40 5 20 3,9 *",
)

CC_HH_INTERNET_ACCESS_BY_AGE_BY_TRACT = CensusVariableGroupDataset(
    dataset_name="cc_hh_internet_access_by_age_by_tract",
    api_call_obj=CensusVariableGroupAPICall(
        dataset_base_url="http://api.census.gov/data/2021/acs/acs5",
        group_name="B28005",
        geographies=COOK_COUNTY_CENSUS_TRACTS,
    ),
    schedule="50 5 20 3,9 *",
)

CC_PLANNING_DB_HOUSING_AND_DEMOS_BY_BG = CensusVariableGroupDataset(
    dataset_name="cc_planning_db_housing_and_demos_by_bg",
    api_call_obj=CensusDatasetVariablesAPICaller(
        dataset_base_url="http://api.census.gov/data/2020/pdb/blockgroup",
        geographies=COOK_COUNTY_CENSUS_BLOCK_GROUPS,
        variable_names=[
            "State",
            "County",
            "Tract",
            "Block_group",
            "GIDBG",
            "Renter_Occp_HU_CEN_2010",
            "Renter_Occp_HU_ACS_14_18",
            "Owner_Occp_HU_ACS_14_18",
            "Single_Unit_ACS_14_18",
            "avg_Tot_Prns_in_HHD_CEN_2010",
            "Tot_Housing_Units_CEN_2010",
            "Tot_Vacant_Units_CEN_2010",
            "Tot_Occp_Units_CEN_2010",
            "avg_Tot_Prns_in_HHD_ACS_14_18",
            "Tot_Housing_Units_ACS_14_18",
            "Tot_Vacant_Units_ACS_14_18",
            "Tot_Occp_Units_ACS_14_18",
            "MLT_U2_9_STRC_ACS_14_18",
            "MLT_U10p_ACS_14_18",
            "No_Plumb_ACS_14_18",
            "Recent_Built_HU_ACS_14_18",
            "Tot_Population_ACS_14_18",
            "Median_Age_ACS_14_18",
            "Pop_65plus_ACS_14_18",
            "Pop_5_17_ACS_14_18",
            "Pop_18_24_ACS_14_18",
            "Pop_25_44_ACS_14_18",
            "Pop_45_64_ACS_14_18",
            "Females_ACS_14_18",
            "Males_ACS_14_18",
            "avg_Agg_HH_INC_ACS_14_18",
            "Aggregate_HH_INC_ACS_14_18",
            "Med_HHD_Inc_BG_ACS_14_18",
            "PUB_ASST_INC_ACS_14_18",
            "Diff_HU_1yr_Ago_ACS_14_18",
            "HHD_Moved_in_ACS_14_18",
            "HHD_PPL_Und_18_ACS_14_18",
            "Female_No_HB_CEN_2010",
            "Rel_Family_HHD_CEN_2010",
            "NonFamily_HHD_ACS_14_18",
            "Rel_Family_HHD_ACS_14_18",
            "MrdCple_Fmly_HHD_ACS_14_18",
            "Not_MrdCple_HHD_ACS_14_18",
            "Female_No_HB_ACS_14_18",
            "Sngl_Prns_HHD_ACS_14_18",
        ],
    ),
    schedule="40 5 15 2,6,10 *",
)

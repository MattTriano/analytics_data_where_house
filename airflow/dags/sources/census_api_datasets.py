from dataclasses import dataclass
from typing import Optional

from cc_utils.census import CensusVariableGroupAPICall, CensusGeogTract


@dataclass
class CensusVariableGroupDataset:
    dataset_name: str
    api_call_obj: CensusVariableGroupAPICall
    schedule: Optional[str] = None


GROSS_RENT_BY_ILLINOIS_TRACT = CensusVariableGroupDataset(
    dataset_name="gross_rent_by_illinois_tract",
    api_call_obj=CensusVariableGroupAPICall(
        api_base_url="http://api.census.gov/data/2021/acs/acs5",
        identifier="https://api.census.gov/data/id/ACSDT5Y2021",
        group_name="B25063",
        geographies=CensusGeogTract(state_cd="17"),
    ),
    schedule="0 2 10 4,10 *",
)

GROSS_RENT_BY_COOK_COUNTY_IL_TRACT = CensusVariableGroupDataset(
    dataset_name="gross_rent_by_cook_county_il_tract",
    api_call_obj=CensusVariableGroupAPICall(
        api_base_url="http://api.census.gov/data/2021/acs/acs5",
        identifier="https://api.census.gov/data/id/ACSDT5Y2021",
        group_name="B25063",
        geographies=CensusGeogTract(state_cd="17", county_cd="031"),
    ),
    schedule="5 2 10 4,10 *",
)

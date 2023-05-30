from dataclasses import dataclass
from typing import Optional

from cc_utils.census import CensusVariableGroupAPICall, CensusGeogTract, CensusAPIHandler


@dataclass
class CensusVariableGroupDataset:
    dataset_name: Optional[str] = None
    api_call_obj: CensusVariableGroupAPICall
    schedule: Optional[str] = None


GROSS_RENT_BY_ILLINOIS_TRACT = CensusVariableGroupDataset(
    dataset_name="gross_rent_by_illinois_tract",
    api_call_obj=CensusVariableGroupAPICall(
        identifier="https://api.census.gov/data/id/ACSDT5Y2021",
        group_name="B25063",
        geographies=CensusGeogTract(state_cd="17"),
        api_handler=CensusAPIHandler(),
    ),
    schedule="0 2 10 4,10 *",
)

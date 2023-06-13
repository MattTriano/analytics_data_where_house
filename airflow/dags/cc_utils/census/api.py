from dataclasses import dataclass
import datetime as dt
import os
import re
from typing import List, Optional, Protocol, Union

import requests
import pandas as pd

from cc_utils.census.core import (
    get_dataset_metadata_catalog,
    get_dataset_variables_metadata,
    get_dataset_geography_metadata,
    get_dataset_groups_metadata,
    get_dataset_tags_metadata,
)


class CensusGeography:
    def format_geog_cd_param(self, geog_cd: Union[List, str]) -> str:
        if isinstance(geog_cd, list):
            return ",".join([el.strip() for el in geog_cd])
        else:
            return geog_cd


class CensusGeogTract(CensusGeography):
    def __init__(self, state_cd: str, county_cd: str = "*"):
        self.state_cd = self.format_geog_cd_param(geog_cd=state_cd)
        self.county_cd = self.format_geog_cd_param(geog_cd=county_cd)

    @property
    def api_call_geographies(self):
        return f"for=tract:*&in=state:{self.state_cd}&in=county:{self.county_cd}"


class CensusGeogBlockGroup(CensusGeography):
    def __init__(
        self,
        state_cd: Union[List, str],
        county_cd: Union[List, str],
        tract_cd: Union[List, str] = "*",
    ):
        self.state_cd = self.format_geog_cd_param(geog_cd=state_cd)
        self.county_cd = self.format_geog_cd_param(geog_cd=county_cd)
        self.tract_cd = self.format_geog_cd_param(geog_cd=tract_cd)

    @property
    def api_call_geographies(self):
        return f"for=block%20group:*&in=state:{self.state_cd}&in=county:{self.county_cd}&in=tract:{self.tract_cd}"


class CensusAPIDataset(Protocol):
    def api_call(self) -> str:
        ...

    def make_api_call(self) -> pd.DataFrame:
        ...


class CensusVariableGroupAPICall(CensusAPIDataset):
    def __init__(
        self,
        dataset_base_url: str,
        group_name: str,
        geographies: CensusGeogTract,
    ):
        self.dataset_base_url = dataset_base_url
        self.group_name = group_name
        self.geographies = geographies

    @property
    def api_call(self) -> str:
        base_url = self.dataset_base_url
        group_part = f"group({self.group_name})"
        geog_part = self.geographies.api_call_geographies
        auth_part = f"""key={os.environ["CENSUS_API_KEY"]}"""
        return f"{base_url}?get={group_part}&{geog_part}&{auth_part}"

    def make_api_call(self) -> pd.DataFrame:
        resp = requests.get(self.api_call)
        if resp.status_code == 200:
            resp_json = resp.json()
            return pd.DataFrame(resp_json[1:], columns=resp_json[0])
        else:
            raise Exception(f"The API call produced an invalid response ({resp.status_code})")


class CensusDatasetVariablesAPICaller(CensusAPIDataset):
    def __init__(
        self,
        dataset_base_url: str,
        variable_names: List[str],
        geographies: CensusGeography,
    ):
        self.dataset_base_url = dataset_base_url
        self.variable_names = variable_names
        self.geographies = geographies
        self.validate_variables()

    def validate_variables(self):
        if len(self.variable_names) > 50:
            raise Exception("Received too many variables for a Census API call")

    @property
    def api_call(self) -> str:
        base_url = self.dataset_base_url
        vars_part = f"""{",".join(self.variable_names)}"""
        geog_part = self.geographies.api_call_geographies
        auth_part = f"""key={os.environ["CENSUS_API_KEY"]}"""
        return f"{base_url}?get={vars_part}&{geog_part}&{auth_part}"

    def make_api_call(self) -> pd.DataFrame:
        resp = requests.get(self.api_call)
        if resp.status_code == 200:
            resp_json = resp.json()
            return pd.DataFrame(resp_json[1:], columns=resp_json[0])
        else:
            raise Exception(f"The API call produced an invalid response ({resp.status_code})")


@dataclass
class CensusVariableGroupDataset:
    dataset_name: str
    api_call_obj: CensusVariableGroupAPICall
    schedule: Optional[str] = None


class CensusAPIDatasetSource:
    def __init__(self, dataset_base_url: str):
        self.base_url = dataset_base_url
        self.metadata_catalog_df = get_dataset_metadata_catalog(dataset_base_url=self.base_url)
        self.set_dataset_metadata_urls()
        self.variables_df = get_dataset_variables_metadata(variables_url=self.variables_url)
        self.geographies_df = get_dataset_geography_metadata(geog_url=self.geographies_url)
        self.groups_df = get_dataset_groups_metadata(groups_url=self.groups_url)
        self.tags_df = get_dataset_tags_metadata(tags_url=self.tags_url)
        self.time_of_check = dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    def set_dataset_metadata_urls(self):
        if (self.metadata_catalog_df["dataset_base_url"] == self.base_url).sum() == 0:
            if self.base_url.startswith("https://"):
                base_url = re.sub("https://", "http://", self.base_url)
            elif self.base_url.startswith("http://"):
                base_url = re.sub("http://", "https://", self.base_url)
            else:
                raise Exception(
                    f"bad base_url. How did we get past the dataset_metadata_catalog network"
                    + " request?"
                )
        else:
            base_url = self.base_url
        dataset_metadata_df = self.metadata_catalog_df.loc[
            self.metadata_catalog_df["dataset_base_url"] == base_url
        ].copy()
        self.geographies_url = dataset_metadata_df["geography_link"].iloc[0]
        self.variables_url = dataset_metadata_df["variables_link"].iloc[0]
        self.groups_url = dataset_metadata_df["groups_link"].iloc[0]
        self.tags_url = dataset_metadata_df["tags_link"].iloc[0]


class CensusDatasetFreshnessCheck:
    def __init__(
        self,
        dataset_source: CensusAPIDatasetSource,
        source_freshness: pd.DataFrame,
        local_freshness: pd.DataFrame,
    ):
        self.dataset_source = dataset_source
        self.source_freshness = source_freshness
        self.local_freshness = local_freshness

from collections import Counter
import datetime as dt
from itertools import chain
from logging import Logger
import os
from random import random
import re
import requests
from time import sleep
from typing import Dict, List, Tuple, Union, Optional, Protocol

from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import select, insert, update
from sqlalchemy.engine.base import Engine

from cc_utils.db import (
    get_pg_engine,
    execute_result_returning_query,
    get_reflected_db_table,
    execute_result_returning_orm_query,
)


def check_for_updated_census_table_metadata(engine: Engine):
    page_obj = CensusTableMetadata(metadata_url="https://www2.census.gov/")
    update_df = page_obj.check_warehouse_data_freshness(engine=engine)
    _ = page_obj.insert_current_metadata_freshness_check(engine=engine)
    to_check_mask = update_df["is_dir"] & update_df["updated_metadata_available"]

    metadata_urls_to_check = []
    metadata_urls_to_check.extend(list(update_df.loc[to_check_mask, "metadata_url"]))
    while len(metadata_urls_to_check) > 0:
        metadata_url = metadata_urls_to_check.pop()
        print(f"Checking {metadata_url}")
        sleep_time = 0.5 * random() + 0.5
        sleep(sleep_time)
        page_obj = CensusTableMetadata(metadata_url=metadata_url)
        update_df = page_obj.check_warehouse_data_freshness(engine=engine)
        _ = page_obj.insert_current_metadata_freshness_check(engine=engine)
        to_check_mask = update_df["is_dir"] & update_df["updated_metadata_available"]
        metadata_urls_to_check.extend(list(update_df.loc[to_check_mask, "metadata_url"]))


class CensusTableMetadata:
    BASE_URL = "https://www2.census.gov"

    def __init__(self, metadata_url: str = BASE_URL):
        self.metadata_url = re.sub("/$", "", metadata_url)
        self.page_metadata = self.get_page_metadata()
        self.time_of_check = dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        self.data_freshness_check = self.initialize_data_freshness_check()

    def parse_census_metadata_page(self, resp: requests.models.Response) -> pd.DataFrame:
        soup = BeautifulSoup(resp.content, "html.parser")
        table = soup.find("table")
        rows = table.find_all("tr")
        table_contents = []
        for row in rows:
            cols = row.find_all("td")
            cols = [col.text.strip() for col in cols]
            table_contents.append(cols)
        table_rows = [el for el in table_contents if len(el) > 0]

        metadata_df = pd.DataFrame(
            [row[1:] for row in table_rows],
            columns=["name", "last_modified", "size", "description"],
        )
        metadata_df["last_modified"] = pd.to_datetime(metadata_df["last_modified"])
        metadata_df["is_dir"] = metadata_df["name"].str.endswith("/")
        metadata_df["clean_name"] = metadata_df["name"].str.replace("/$", "", regex=True)
        metadata_df["is_file"] = (~metadata_df["is_dir"]) & (
            metadata_df["clean_name"] != "Parent Directory"
        )
        mask = metadata_df["is_file"] | metadata_df["is_dir"]
        metadata_df.loc[mask, "metadata_url"] = (
            self.metadata_url + "/" + metadata_df.loc[mask, "clean_name"]
        )
        return metadata_df

    def get_page_metadata(self):
        resp = requests.get(self.metadata_url)
        if resp.status_code == 200:
            metadata_df = self.parse_census_metadata_page(resp)
        else:
            raise Exception(f"Couldn't get page metadata for url {self.metadata_url}")
        return metadata_df

    @property
    def child_file_urls(self):
        if len(self.page_metadata) and all(self.page_metadata["clean_name"] == "Parent Directory"):
            return []
        else:
            return [
                f"{self.metadata_url}/{fn}"
                for fn in self.page_metadata.loc[self.page_metadata["is_file"], "clean_name"]
            ]

    @property
    def child_dir_urls(self):
        if len(self.page_metadata) and all(self.page_metadata["clean_name"] == "Parent Directory"):
            return []
        else:
            return [
                f"{self.metadata_url}/{fn}"
                for fn in self.page_metadata.loc[self.page_metadata["is_dir"], "clean_name"]
            ]

    @property
    def last_modified_child(self):
        return self.page_metadata["last_modified"].max()

    def initialize_data_freshness_check(self) -> pd.DataFrame:
        col_order = [
            "metadata_url",
            "last_modified",
            "size",
            "description",
            "is_dir",
            "is_file",
        ]
        df = self.page_metadata[col_order].copy()
        df["time_of_check"] = self.time_of_check
        df = df.loc[df["metadata_url"].notnull()].copy()
        df = df.reset_index(drop=True)
        return df

    def get_prior_metadata_checks_from_db(self, engine: Engine) -> pd.DataFrame:
        metadata_urls = f"""{"', '".join([
            el.replace("'", "''") for el in list(self.data_freshness_check["metadata_url"])
        ])}"""
        results_df = execute_result_returning_query(
            query=f"""
                SELECT *
                FROM metadata.census_metadata
                WHERE metadata_url IN ('{metadata_urls}');
            """,
            engine=engine,
        )
        return results_df

    def check_warehouse_data_freshness(self, engine: Engine):
        check_df = self.get_prior_metadata_checks_from_db(engine=engine)
        check_df = check_df.sort_values(
            by=["metadata_url", "last_modified"],
            ascending=[True, True],
            ignore_index=True,
        )
        check_df = check_df.drop_duplicates(subset="metadata_url", keep="last", ignore_index=True)
        output_cols = list(self.data_freshness_check.columns)
        output_cols.append("updated_metadata_available")
        update_avail_df = pd.merge(
            left=self.data_freshness_check,
            right=check_df[["metadata_url", "last_modified"]],
            how="left",
            on="metadata_url",
            suffixes=("_source", "_cache"),
        )
        update_avail_df["updated_metadata_available"] = update_avail_df[
            "last_modified_cache"
        ].isnull() | (
            update_avail_df["last_modified_source"] > update_avail_df["last_modified_cache"]
        )
        update_avail_df = update_avail_df.rename(columns={"last_modified_source": "last_modified"})
        update_avail_df = update_avail_df[output_cols].copy()
        return update_avail_df.copy()

    def insert_current_metadata_freshness_check(self, engine):
        update_df = self.check_warehouse_data_freshness(engine=engine)
        if any(update_df["updated_metadata_available"]):
            metadata_table = get_reflected_db_table(
                engine=engine, table_name="census_metadata", schema_name="metadata"
            )
            insert_statement = (
                insert(metadata_table)
                .values(update_df.to_dict(orient="records"))
                .returning(metadata_table)
            )
            result_df = execute_result_returning_orm_query(
                engine=engine, select_query=insert_statement
            )
            return result_df


class CensusDatasetSource:
    def __init__(self, identifier: str, base_api_call: str, media_type: str = "json"):
        self.identifier = identifier
        self.base_api_call = base_api_call
        self.media_type = media_type

    def get_detail_url(self, detail_type: str) -> str:
        return f"{self.base_api_call}/{detail_type}.{self.media_type}"

    @property
    def variables_url(self):
        return self.get_detail_url(detail_type="variables")

    @property
    def examples_url(self):
        return self.get_detail_url(detail_type="examples")

    @property
    def sorts_url(self):
        return self.get_detail_url(detail_type="sorts")

    @property
    def geographies_url(self):
        return self.get_detail_url(detail_type="geography")

    @property
    def tags_url(self):
        return self.get_detail_url(detail_type="tags")

    @property
    def groups_url(self):
        return self.get_detail_url(detail_type="groups")

    def get_url_response(self, url: str) -> Dict:
        api_call = re.sub("\.html$", ".json", url)
        resp = requests.get(api_call)
        if resp.status_code == 200:
            resp_json = resp.json()
            return resp_json
        else:
            print(f"Failed to get a valid response; status code: {resp.status_code}")
            return None

    @property
    def variables_df(self) -> None:
        variables_resp_json = self.get_url_response(self.variables_url)
        variables_df = pd.DataFrame(variables_resp_json["variables"]).T
        variables_df.index.name = "variable"
        variables_df = variables_df.reset_index()
        variables_df["identifier"] = self.identifier
        var_col_namemap = {
            "identifier": "identifier",
            "variable": "variable",
            "label": "label",
            "concept": "concept",
            "predicateType": "predicate_type",
            "group": "dataset_group",
            "limit": "limit_call",
            "predicateOnly": "predicate_only",
            "hasGeoCollectionSupport": "has_geo_collection_support",
            "attributes": "attributes",
            "required": "required",
            "values": "values",
            "datetime": "datetime",
            "is-weight": "is_weight",
            "suggested-weight": "suggested_weight",
        }
        variables_df = variables_df.rename(columns=var_col_namemap)
        if "values" in variables_df.columns:
            variables_df["values"] = variables_df["values"].fillna({})
        variables_df["predicate_only"] = variables_df["predicate_only"].fillna(False)
        col_order = [col for col in var_col_namemap.values() if col in variables_df.columns]
        variables_df = variables_df[col_order].copy()
        return variables_df

    @property
    def geographies_df(self) -> None:
        geo_resp_json = self.get_url_response(self.geographies_url)
        geographies_df = pd.DataFrame(geo_resp_json["fips"])
        geo_col_namemap = {
            "name": "name",
            "geoLevelDisplay": "geo_level",
            "referenceDate": "reference_date",
            "requires": "requires",
            "wildcard": "wildcard",
            "optionalWithWCFor": "optional_with_wildcard_for",
        }
        geographies_df = geographies_df.rename(columns=geo_col_namemap)
        return geographies_df

    @property
    def groups_df(self) -> None:
        groups_resp_json = self.get_url_response(self.groups_url)
        groups_df = pd.DataFrame(groups_resp_json["groups"])
        groups_df.columns = [col.strip() for col in groups_df.columns]
        groups_df = groups_df.rename(
            columns={
                "name": "group_name",
                "description": "group_description",
                "variables": "group_variables",
            }
        )
        return groups_df


class CensusAPICatalog:
    def __init__(self, metadata_df: Optional[pd.DataFrame] = None):
        if metadata_df is not None:
            self.validate_reloaded_dataset_metadata(metadata_df=metadata_df)
        else:
            self.set_dataset_metadata()

    def set_data_catalog_json(self) -> pd.DataFrame:
        url = "https://api.census.gov/data.json"
        resp = requests.get(url)

        if resp.status_code == 200:
            data_catalog_json = resp.json()
            self.data_catalog_json = data_catalog_json
        else:
            raise Exception(f"Failed to get a valid response; status_code: {resp.status_code}")

    def set_dataset_metadata(self) -> None:
        self.set_data_catalog_json()
        if "dataset" in self.data_catalog_json.keys():
            datasets = self.data_catalog_json["dataset"]
            print(f"Elements in Census data catalog datasets attr: {len(datasets)} ")
            df_list = []
            df_shape_list = []
            for dataset in datasets:
                df = pd.json_normalize(dataset)
                df_list.append(df)
                df_shape_list.append(df.shape)
            full_df = pd.concat(df_list)
            full_df = full_df.reset_index(drop=True)
            full_df["modified"] = pd.to_datetime(full_df["modified"])
            distribution_df = pd.json_normalize(full_df["distribution"].str[0])
            distribution_df.columns = [f"distribution_{col}" for col in distribution_df.columns]
            full_df = pd.merge(
                left=full_df,
                right=distribution_df,
                how="left",
                left_index=True,
                right_index=True,
            )
            full_df = full_df.sort_values(by="modified", ascending=False, ignore_index=True)
            colname_fixes = {
                "identifier": "identifier",
                "title": "title",
                "description": "description",
                "modified": "modified",
                "c_vintage": "vintage",
                "distribution_accessURL": "distribution_access_url",
                "c_geographyLink": "geography_link",
                "c_variablesLink": "variables_link",
                "c_tagsLink": "tags_link",
                "c_examplesLink": "examples_link",
                "c_groupsLink": "groups_link",
                "c_sorts_url": "sorts_url",
                "c_dataset": "dataset",
                "spatial": "spatial",
                "temporal": "temporal",
                "bureauCode": "bureau_code",
                "programCode": "program_code",
                "keyword": "keyword",
                "c_isMicrodata": "is_microdata",
                "c_isAggregate": "is_aggregate",
                "c_isCube": "is_cube",
                "c_isAvailable": "is_available",
                "c_isTimeseries": "is_timeseries",
                "accessLevel": "access_level",
                "license": "license",
                "@type": "type",
                "publisher.name": "publisher_name",
                "publisher.@type": "publisher_type",
                "contactPoint.fn": "contact_point_fn",
                "contactPoint.hasEmail": "contact_point_email",
                "distribution_@type": "distribution_type",
                "distribution_mediaType": "distribution_media_type",
                "references": "reference_docs",
                "c_documentationLink": "documentation_link",
                "distribution": "distribution",
                "distribution_description": "distribution_description",
                "distribution_format": "distribution_format",
                "distribution_title": "distribution_title",
                "publisher.subOrganizationOf.@type": "publisher_suborg_of_type",
                "publisher.subOrganizationOf.name": "publisher_suborg_of_name",
                "publisher.subOrganizationOf.subOrganizationOf.@type": "publisher_suborg_of_suborg_of_type",
                "publisher.subOrganizationOf.subOrganizationOf.name": "publisher_suborg_of_suborg_of_name",
            }
            dataset_metadata = full_df[colname_fixes.keys()].copy()
            dataset_metadata = dataset_metadata.rename(columns=colname_fixes)
            self.dataset_metadata = dataset_metadata
        else:
            raise Exception(f"field 'dataset' not found in data_catalog response")

    def validate_reloaded_dataset_metadata(self, metadata_df: pd.DataFrame) -> None:
        essential_cols = [
            "identifier",
            "title",
            "description",
            "modified",
            "vintage",
            "distribution_access_url",
            "geography_link",
            "variables_link",
            "tags_link",
            "examples_link",
            "groups_link",
            "sorts_url",
            "dataset",
            "spatial",
            "temporal",
            "bureau_code",
            "program_code",
            "keyword",
            "is_microdata",
            "is_aggregate",
            "is_cube",
            "is_available",
            "is_timeseries",
            "time_of_check",
        ]
        if all(col in metadata_df.columns for col in essential_cols):
            self.dataset_metadata = metadata_df
        else:
            raise Exception(f"field 'dataset' not found in data_catalog response")

    def get_counts_of_nested_data_elements(self, key: str = "dataset") -> List[Tuple]:
        label_counts = Counter(chain.from_iterable(self.dataset_metadata[key]))
        label_counts = sorted(label_counts.items(), key=lambda x: x[1], reverse=True)
        return label_counts

    def get_dataset_source(self, identifier: str, media_type: str = "json") -> CensusDatasetSource:
        base_api_call = self.dataset_metadata.loc[
            self.dataset_metadata["identifier"] == identifier, "distribution_access_url"
        ].values[0]
        return CensusDatasetSource(
            identifier=identifier, base_api_call=base_api_call, media_type=media_type
        )

    def standardize_datetime_str_repr(self, datetime_obj: Union[str, dt.datetime]) -> str:
        if isinstance(datetime_obj, str):
            datetime_obj = dt.datetime.strptime(datetime_obj, "%Y-%m-%dT%H:%M:%S.%fZ")
        return datetime_obj.strftime("%Y-%m-%dT%H:%M:%SZ")


class CensusAPIHandler:
    def __init__(self, conn_id: str, task_logger: Logger, max_days_before_refresh: int = 30):
        self.conn_id = conn_id
        self.task_logger = task_logger
        self.max_days_before_refresh = max_days_before_refresh
        self.local_metadata_df = self.get_freshest_census_API_data_catalog_metadata()
        self.set_catalog()

    def get_freshest_census_API_data_catalog_metadata(self) -> pd.DataFrame:
        engine = get_pg_engine(conn_id=self.conn_id)
        catalog_metadata_df = execute_result_returning_query(
            engine=engine,
            query="""
                WITH latest_metadata AS (
                    SELECT
                        *,
                        row_number() over(
                            partition by identifier, modified ORDER BY identifier, modified DESC
                        ) as rn
                    FROM metadata.census_api_metadata
                )
                SELECT *
                FROM latest_metadata
                WHERE rn = 1;
            """,
        )
        return catalog_metadata_df

    def get_freshest_local_dataset_metadata(self, dataset_name) -> pd.DataFrame:
        engine = get_pg_engine(conn_id=self.conn_id)
        local_dataset_metadata_df = execute_result_returning_query(
            engine=engine,
            query=f"""
                WITH latest_metadata AS (
                    SELECT
                        *,
                        row_number() over(
                            partition by dataset_name, source_data_last_modified ORDER BY dataset_name, source_data_last_modified DESC
                        ) as rn
                    FROM metadata.dataset_metadata
                    WHERE dataset_name = '{dataset_name}'
                )
                SELECT *
                FROM latest_metadata
                WHERE rn = 1;
            """,
        )
        return local_dataset_metadata_df

    def ingest_api_dataset_freshness_check(self, metadata_df: pd.DataFrame) -> None:
        engine = get_pg_engine(conn_id=self.conn_id)
        api_dataset_metadata_table = get_reflected_db_table(
            engine=engine, table_name="census_api_metadata", schema_name="metadata"
        )
        insert_statement = (
            insert(api_dataset_metadata_table)
            .values(metadata_df.to_dict(orient="records"))
            .returning(api_dataset_metadata_table)
        )
        ingested_api_datasets_df = execute_result_returning_orm_query(
            engine=engine, select_query=insert_statement
        )
        self.task_logger.info(
            f"Max census_api_metadata id value after ingestion: {ingested_api_datasets_df['id'].max()}"
        )
        return ingested_api_datasets_df

    def set_catalog(self) -> None:
        latest_check = self.local_metadata_df["time_of_check"].max()
        time_since_latest_check = (pd.Timestamp.now(tz=latest_check.tz) - latest_check).days
        if time_since_latest_check > self.max_days_before_refresh:
            self.catalog = CensusAPICatalog()
            self.time_of_check = dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            self.prepare_dataset_metadata_df()
            self.ingest_api_dataset_freshness_check(metadata_df=self.metadata_df)
        else:
            self.catalog = CensusAPICatalog(metadata_df=self.local_metadata_df)
            self.metadata_df = self.local_metadata_df.copy()

    def is_local_dataset_metadata_fresh(self, identifier: str) -> bool:
        in_local_mask = self.local_metadata_df["identifier"] == identifier
        in_source_mask = self.metadata_df["identifier"] == identifier
        if any(in_local_mask):
            dataset_local_df = self.local_metadata_df.loc[in_local_mask].copy()
        if any(in_source_mask):
            dataset_source_df = self.metadata_df.loc[in_source_mask].copy()
        if any(in_local_mask) and any(in_source_mask):
            return dataset_local_df["modified"].values[0] >= dataset_source_df["modified"].values[0]
        elif any(in_source_mask):
            return False
        else:
            raise Exception(f"Somehow dataset {identifier} is in local db but not on Census site.")

    def prepare_dataset_metadata_df(self):
        metadata_df = self.catalog.dataset_metadata.copy()
        drop_cols = [
            "distribution",
            "distribution_description",
            "distribution_format",
            "distribution_title",
            "publisher_suborg_of_type",
            "publisher_suborg_of_name",
            "publisher_suborg_of_suborg_of_type",
            "publisher_suborg_of_suborg_of_name",
        ]
        drop_cols = [col for col in drop_cols if col in metadata_df.columns]
        if len(drop_cols) > 0:
            metadata_df = metadata_df.drop(columns=drop_cols)

        bool_cols = [
            "is_microdata",
            "is_aggregate",
            "is_cube",
            "is_timeseries",
            "is_available",
        ]
        for bool_col in bool_cols:
            metadata_df[bool_col] = metadata_df[bool_col].fillna(False).astype(bool)

        mask = metadata_df["vintage"].isnull()
        metadata_df["vintage"] = metadata_df["vintage"].fillna(-1).astype(int).astype(str)
        metadata_df.loc[mask, "vintage"] = None
        metadata_df["time_of_check"] = self.time_of_check
        self.metadata_df = metadata_df.copy()

    def prepare_dataset_variables_metadata_df(self, identifier: str) -> pd.DataFrame:
        dataset_source = self.catalog.get_dataset_source(identifier=identifier)
        variables_df = dataset_source.variables_df.copy()
        col_order = ["dataset_id"]
        col_order.extend(list(variables_df.columns))
        col_order.extend(["dataset_last_modified", "time_of_check"])
        dataset_metadata_df = self.catalog.dataset_metadata.loc[
            self.catalog.dataset_metadata["identifier"] == identifier
        ].copy()
        dataset_metadata_df = dataset_metadata_df.sort_values(by="time_of_check", ascending=False)
        variables_df["dataset_id"] = dataset_metadata_df["id"].values[0]
        variables_df["dataset_last_modified"] = pd.Timestamp(
            dataset_metadata_df["modified"].values[0]
        )
        variables_df["time_of_check"] = pd.Timestamp(dataset_metadata_df["time_of_check"].values[0])
        variables_df = variables_df[col_order].copy()
        variables_df = variables_df.where(pd.notnull(variables_df), None)
        return variables_df

    def prepare_dataset_geographies_metadata_df(self, identifier: str) -> pd.DataFrame:
        identifier_mask = self.catalog.dataset_metadata["identifier"] == identifier
        if not any(identifier_mask):
            raise Exception(f"No dataset with identifier {identifier} found in metadata.")
        dataset_source = self.catalog.get_dataset_source(identifier=identifier)
        geographies_df = dataset_source.geographies_df.copy()
        col_order = ["dataset_id", "identifier"]
        col_order.extend(list(geographies_df.columns))
        col_order.extend(["dataset_last_modified", "time_of_check"])
        dataset_metadata_df = self.catalog.dataset_metadata.loc[
            self.catalog.dataset_metadata["identifier"] == identifier
        ].copy()
        geographies_df["dataset_id"] = dataset_metadata_df["id"].values[0]
        geographies_df["identifier"] = identifier
        geographies_df["dataset_last_modified"] = pd.Timestamp(
            dataset_metadata_df["modified"].values[0]
        )
        geographies_df["time_of_check"] = pd.Timestamp(
            dataset_metadata_df["time_of_check"].values[0]
        )
        geographies_df = geographies_df[col_order].copy()
        geographies_df = geographies_df.where(pd.notnull(geographies_df), None)
        return geographies_df

    def prepare_dataset_groups_metadata_df(self, identifier: str) -> pd.DataFrame:
        identifier_mask = self.catalog.dataset_metadata["identifier"] == identifier
        if not any(identifier_mask):
            raise Exception(f"No dataset with identifier {identifier} found in metadata.")
        dataset_source = self.catalog.get_dataset_source(identifier=identifier)
        groups_df = dataset_source.groups_df.copy()
        col_order = ["dataset_id", "identifier"]
        col_order.extend([col.strip() for col in groups_df.columns])
        col_order.extend(["time_of_check"])
        dataset_metadata_df = self.catalog.dataset_metadata.loc[
            self.catalog.dataset_metadata["identifier"] == identifier
        ].copy()
        groups_df["dataset_id"] = dataset_metadata_df["id"].values[0]
        groups_df["identifier"] = identifier
        groups_df["time_of_check"] = pd.Timestamp(dataset_metadata_df["time_of_check"].values[0])
        groups_df = groups_df[col_order].copy()
        groups_df = groups_df.where(pd.notnull(groups_df), None)
        return groups_df


class CensusGeogTract:
    def __init__(self, state_cd: str, county_cd: str = "*"):
        self.state_cd = state_cd
        self.county_cd = county_cd

    @property
    def api_call_geographies(self):
        return f"for=tract:*&in=state:{self.state_cd}&in=county:{self.county_cd}"


class CensusAPIDataset(Protocol):
    def api_call(self) -> str:
        ...

    def make_api_call(self) -> pd.DataFrame:
        ...


class CensusVariableGroupAPICall(CensusAPIDataset):
    def __init__(
        self,
        api_base_url: str,
        identifier: str,
        group_name: str,
        geographies: CensusGeogTract,
    ):
        self.api_base_url = api_base_url
        self.identifier = identifier
        self.group_name = group_name
        self.geographies = geographies

    @property
    def api_call(self) -> str:
        base_url = self.api_base_url
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


def get_url_response(url: str) -> Dict:
    api_call = re.sub("\.html$", ".json", url)
    resp = requests.get(api_call)
    if resp.status_code == 200:
        resp_json = resp.json()
        return resp_json
    else:
        print(f"Failed to get a valid response; status code: {resp.status_code}")
        return None


def get_dataset_metadata_catalog(base_dataset_url: str) -> pd.DataFrame:
    catalog_resp_json = get_url_response(url=base_dataset_url)
    if catalog_resp_json is None:
        raise Exception(
            f"Request for metadata catalog for the dataset with url\n\n  {base_dataset_url}\n\n"
            + "failed to get a valid response."
        )
    catalog_resp_df = pd.json_normalize(catalog_resp_json)
    catalog_cols = [col for col in catalog_resp_df.columns if col != "dataset"]
    catalog_df = catalog_resp_df[catalog_cols].copy()
    datasets = catalog_resp_df["dataset"].iloc[0].copy()
    catalog_colname_fixes = {
        "@context": "metadata_context",
        "@id": "metadata_catalog_id",
        "@type": "metadata_type",
        "conformsTo": "conforms_to_schema",
        "describedBy": "data_schema_dictionary",
    }
    catalog_df = catalog_df.rename(columns=catalog_colname_fixes)
    print(f"Elements in Census data catalog datasets attr: {len(datasets)} ")
    df_list = []
    df_shape_list = []
    for dataset in datasets:
        df = pd.json_normalize(dataset)
        df_list.append(df)
        df_shape_list.append(df.shape)
    datasets_df = pd.concat(df_list)
    datasets_df = datasets_df.reset_index(drop=True)
    datasets_df["modified"] = pd.to_datetime(datasets_df["modified"])
    distribution_df = pd.json_normalize(datasets_df["distribution"].str[0])
    distribution_df.columns = [f"distribution_{col}" for col in distribution_df.columns]
    datasets_df = pd.merge(
        left=datasets_df,
        right=distribution_df,
        how="left",
        left_index=True,
        right_index=True,
    )
    datasets_df = datasets_df.sort_values(by="modified", ascending=False, ignore_index=True)

    datasets_df["join_col"] = 1
    catalog_df["join_col"] = 1
    full_df = pd.merge(left=datasets_df, right=catalog_df, how="inner", on="join_col")
    full_df = full_df.drop(columns=["join_col"])
    colname_fixes = {
        "distribution_accessURL": "dataset_base_url",
        "identifier": "identifier",
        "title": "title",
        "description": "description",
        "modified": "modified",
        "c_vintage": "vintage",
        "c_geographyLink": "geography_link",
        "c_variablesLink": "variables_link",
        "c_tagsLink": "tags_link",
        "c_examplesLink": "examples_link",
        "c_groupsLink": "groups_link",
        "c_sorts_url": "sorts_url",
        "c_dataset": "dataset",
        "spatial": "spatial",
        "temporal": "temporal",
        "bureauCode": "bureau_code",
        "programCode": "program_code",
        "keyword": "keyword",
        "c_isMicrodata": "is_microdata",
        "c_isAggregate": "is_aggregate",
        "c_isCube": "is_cube",
        "c_isAvailable": "is_available",
        "c_isTimeseries": "is_timeseries",
        "accessLevel": "access_level",
        "license": "license",
        "@type": "type",
        "publisher.name": "publisher_name",
        "publisher.@type": "publisher_type",
        "contactPoint.fn": "contact_point_fn",
        "contactPoint.hasEmail": "contact_point_email",
        "distribution_@type": "distribution_type",
        "distribution_mediaType": "distribution_media_type",
        "references": "reference_docs",
        "c_documentationLink": "documentation_link",
        "distribution": "distribution",
        "distribution_description": "distribution_description",
        "distribution_format": "distribution_format",
        "distribution_title": "distribution_title",
        "publisher.subOrganizationOf.@type": "publisher_suborg_of_type",
        "publisher.subOrganizationOf.name": "publisher_suborg_of_name",
        "publisher.subOrganizationOf.subOrganizationOf.@type": "publisher_suborg_of_suborg_of_type",
        "publisher.subOrganizationOf.subOrganizationOf.name": "publisher_suborg_of_suborg_of_name",
    }
    colname_fixes.update({v: v for k, v in catalog_colname_fixes.items()})
    full_df_cols = full_df.columns
    col_order = [col for col in colname_fixes.keys() if col in full_df_cols]
    full_df = full_df[col_order].copy()
    full_df = full_df.rename(columns=colname_fixes)
    return full_df


def get_dataset_geography_metadata(geog_url: str) -> pd.DataFrame:
    geo_resp_json = get_url_response(url=geog_url)
    geographies_df = pd.DataFrame(geo_resp_json["fips"])
    geo_col_namemap = {
        "name": "name",
        "geoLevelDisplay": "geo_level",
        "referenceDate": "reference_date",
        "requires": "requires",
        "wildcard": "wildcard",
        "optionalWithWCFor": "optional_with_wildcard_for",
    }
    geographies_df = geographies_df.rename(columns=geo_col_namemap)
    return geographies_df


class CensusAPIDatasetSource:
    def __init__(self, base_dataset_url: str):
        self.base_url = base_dataset_url
        self.metadata_catalog_df = get_dataset_metadata_catalog(base_dataset_url=self.base_url)
        self.set_dataset_metadata_urls()
        self.geographies_df = get_dataset_geography_metadata(geog_url=self.geographies_url)

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
        dataset_metadata_df = self.metadata_catalog_df.loc[
            self.metadata_catalog_df["dataset_base_url"] == base_url
        ].copy()
        self.geographies_url = dataset_metadata_df["geography_link"].iloc[0]
        self.variables_url = dataset_metadata_df["variables_link"].iloc[0]
        self.groups_url = dataset_metadata_df["groups_link"].iloc[0]
        self.tags_url = dataset_metadata_df["tags_link"].iloc[0]

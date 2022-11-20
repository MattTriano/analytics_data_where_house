from dataclasses import dataclass
import datetime as dt
import json
import re
from pathlib import Path
from typing import Dict, Optional, Union, Tuple

import pandas as pd
import requests
from sqlalchemy.engine.base import Engine
from sqlalchemy import select, insert
from sqlalchemy.orm import Session

# for airflow container
from .db import (
    execute_result_returning_query,
    get_reflected_db_table,
    execute_result_returning_orm_query,
)
from .utils import typeset_zulu_tz_datetime_str

# for interactive dev work
# from db import (
#     execute_result_returning_query,
#     get_reflected_db_table,
#     execute_result_returning_orm_query,
# )
# from utils import typeset_zulu_tz_datetime_str


@dataclass
class SocrataTable:
    table_id: str
    table_name: Optional[str] = None
    download_format: Optional[str] = None


class SocrataTableMetadata:
    def __init__(
        self,
        socrata_table: SocrataTable,
    ):
        self.table_id = socrata_table.table_id
        self.metadata = self.get_table_metadata()
        self.table_name = self.validate_table_name(table_name=socrata_table.table_name)
        self.resource_metadata = self.get_resource_metadata()
        self.column_details = self.get_column_details()
        self.has_geospatial_feature = self.table_has_geospatial_feature()
        self.data_domain = self.get_data_domain()
        self.is_geospatial = self.is_geospatial_table()
        self.download_format = self.validate_download_format(
            download_format=socrata_table.download_format
        )
        self.data_freshness_check = self.initialize_data_freshness_check_record()
        self.freshness_check_id = None

    def get_table_metadata(self) -> Dict:
        api_call = f"http://api.us.socrata.com/api/catalog/v1?ids={self.table_id}"
        response = requests.get(api_call)
        if response.status_code == 200:
            response_json = response.json()
            metadata = {
                "_id": self.table_id,
                "time_of_collection": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            metadata.update(response_json["results"][0])
            return metadata
        else:
            raise Exception(
                f"Request for metadata for table {self.table_id} failed with status code {response.status_code}"
            )

    def get_table_metadata_attr(self, attr_dict: dict, attr_name: str) -> str:
        try:
            if attr_name in attr_dict.keys():
                return attr_dict[attr_name]
            else:
                return None
        except Exception as err:
            print(f"Exception {err} with type {type(err)} raised for attr_name '{attr_name}'.")
            print("Did you mean to enter attr_dict['resource']")
            raise

    def get_resource_metadata(self):
        return self.get_table_metadata_attr(attr_dict=self.metadata, attr_name="resource")

    def validate_table_name(self, table_name: Optional[str]) -> str:
        if table_name is None:
            table_name = self.get_table_metadata_attr(
                a_dict=self.resource_metadata, attr_name="name"
            )
            return "_".join(re.sub("[^0-9a-zA-Z]+", "", table_name.lower()).split())
        else:
            return table_name

    def get_data_domain(self) -> str:
        metadatas_metadata = self.get_table_metadata_attr(
            attr_dict=self.metadata, attr_name="metadata"
        )
        return self.get_table_metadata_attr(attr_dict=metadatas_metadata, attr_name="domain")

    def dump_socrata_metadata_to_json(self, file_path: Path) -> None:
        with open(file_path, "w", encoding="utf-8") as json_file:
            json.dump(self.metadata, json_file, ensure_ascii=False, indent=4, default=str)

    def get_column_details(self) -> Dict:
        if self.resource_metadata is not None:
            column_metadata_fields = [
                "columns_name",
                "columns_field_name",
                "columns_datatype",
                "columns_description",
                "columns_format",
            ]
            present_col_metadata_fields = [
                el for el in column_metadata_fields if el in self.resource_metadata.keys()
            ]
            if len(present_col_metadata_fields) > 0:
                return {
                    field: self.resource_metadata[field] for field in present_col_metadata_fields
                }
        return {}

    def table_has_geospatial_feature(self) -> bool:
        socrata_geo_datatypes = [
            "Line",
            "Location",
            "MultiLine",
            "MultiPoint",
            "MultiPolygon",
            "Point",
            "Polygon",
        ]
        if self.column_details is not None:
            if "columns_datatype" in self.column_details.keys():
                column_datatypes = self.get_table_metadata_attr(
                    attr_dict=self.column_details, attr_name="columns_datatype"
                )
                return any([col_dtype in socrata_geo_datatypes for col_dtype in column_datatypes])
        return False

    def table_has_geo_type_view(self) -> bool:
        table_view_type = self.get_table_metadata_attr(
            attr_dict=self.resource_metadata, attr_name="lens_view_type"
        )
        return table_view_type == "geo"

    def table_has_map_type_display(self) -> bool:
        table_display_type = self.get_table_metadata_attr(
            attr_dict=self.resource_metadata, attr_name="lens_display_type"
        )
        return table_display_type == "map"

    def table_has_data_columns(self) -> bool:
        table_data_cols = self.get_table_metadata_attr(
            attr_dict=self.resource_metadata, attr_name="columns_name"
        )
        return len(table_data_cols) != 0

    def is_geospatial_table(self) -> bool:
        return (
            (not self.table_has_data_columns())
            and (self.table_has_geo_type_view() or self.table_has_map_type_display())
        ) or (self.has_geospatial_feature)

    def get_valid_download_formats(self) -> Dict:
        valid_download_formats = {
            "flat": {
                "CSV": "csv",
                "TSV": "tsv",
            },
            "gis": {
                "shp": "Shapefile",
                "shapefile": "Shapefile",
                "geojson": "GeoJSON",
                "kmz": "KMZ",
                "kml": "KML",
            },
        }
        return valid_download_formats

    def assert_download_format_is_supported(self, download_format: str) -> None:
        valid_download_formats = self.get_valid_download_formats()
        all_pairs = {}
        [all_pairs.update(kv_pairs) for kv_pairs in valid_download_formats.values()]
        if (download_format not in all_pairs.keys()) and (
            download_format not in all_pairs.values()
        ):
            raise Exception(
                f"Download format '{download_format}' isn't supported. Pick from {all_pairs}"
            )

    def validate_download_format(self, download_format: str = None) -> str:
        if download_format is None:
            if self.is_geospatial:
                return "GeoJSON"
            else:
                return "csv"
        else:
            download_format = download_format.lower()
            self.assert_download_format_is_supported(download_format=download_format)
            valid_download_formats = self.get_valid_download_formats()
            if download_format in valid_download_formats.values():
                return download_format
            elif download_format in valid_download_formats.keys():
                return valid_download_formats[download_format]
            else:
                raise Exception("Very invalid download format (should have already been caught)")

    def get_data_download_url(self) -> str:
        if self.is_geospatial:
            return f"https://{self.data_domain}/api/geospatial/{self.table_id}?method=export&format={self.download_format}"
        else:
            return f"https://{self.data_domain}/api/views/{self.table_id}/rows.{self.download_format}?accessType=DOWNLOAD"

    def get_table_classification_metadata(self) -> Dict:
        return self.get_table_metadata_attr(attr_dict=self.metadata, attr_name="classification")

    def get_table_domain_metadata(self) -> Dict:
        classification = self.get_table_classification_metadata()
        if classification is not None:
            domain_metadata = self.get_table_metadata_attr(
                attr_dict=classification, attr_name="domain_metadata"
            )
            if domain_metadata is not None:
                domain_metadata_dict = {}
                for el in domain_metadata:
                    domain_metadata_dict[el["key"]] = el["value"]
                return domain_metadata_dict
        return None

    def standardize_datetime_str_repr(self, datetime_obj: Union[str, dt.datetime]) -> str:
        if isinstance(datetime_obj, str):
            datetime_obj = dt.datetime.strptime(datetime_obj, "%Y-%m-%dT%H:%M:%S.%fZ")
        return datetime_obj.strftime("%Y-%m-%dT%H:%M:%SZ")

    def get_latest_data_update_datetime(self) -> str:
        data_updated_at = self.get_table_metadata_attr(
            attr_dict=self.resource_metadata, attr_name="data_updated_at"
        )
        if data_updated_at is not None:
            return self.standardize_datetime_str_repr(datetime_obj=data_updated_at)
        return None

    def get_latest_metadata_update_datetime(self) -> str:
        metadata_updated_at = self.get_table_metadata_attr(
            attr_dict=self.resource_metadata, attr_name="metadata_updated_at"
        )
        if metadata_updated_at is not None:
            return self.standardize_datetime_str_repr(datetime_obj=metadata_updated_at)
        return None

    def get_prior_metadata_checks_from_db(self, engine: Engine) -> pd.DataFrame:
        results_df = execute_result_returning_query(
            query=f"""
                SELECT *
                FROM metadata.table_metadata
                WHERE table_id = '{self.table_id}';
            """,
            engine=engine,
        )
        return results_df

    def initialize_data_freshness_check_record(self) -> None:
        """There's probably a better name for this idea than 'table_check_metadata'. The goal
        is to see if fresh data is available, log the results of that freshness-check in the dwh,
        and then triger data refreshing if appropriate."""
        return {
            "table_id": self.table_id,
            "table_name": self.table_name,
            "download_format": self.download_format,
            "is_geospatial": self.is_geospatial,
            "data_download_url": self.get_data_download_url(),
            "source_data_last_updated": self.get_latest_data_update_datetime(),
            "source_metadata_last_updated": self.get_latest_metadata_update_datetime(),
            "updated_data_available": None,
            "updated_metadata_available": None,
            "data_pulled_this_check": None,
            "time_of_check": self.metadata["time_of_collection"],
            "metadata_json": self.metadata,
        }

    def check_warehouse_data_freshness(self, engine: Engine):
        check_df = self.get_prior_metadata_checks_from_db(engine=engine)
        self.data_freshness_check["updated_data_available"] = False
        self.data_freshness_check["updated_metadata_available"] = False
        data_pulled_previously_mask = check_df["data_pulled_this_check"] == True
        if (len(check_df) == 0) or (data_pulled_previously_mask.sum() == 0):
            self.data_freshness_check["updated_data_available"] = True
            self.data_freshness_check["updated_metadata_available"] = True
        else:
            latest_pull = check_df.loc[data_pulled_previously_mask, "time_of_check"].max()
            latest_source_data_update = typeset_zulu_tz_datetime_str(
                datetime_str=self.get_latest_data_update_datetime()
            )
            latest_source_metadata_update = typeset_zulu_tz_datetime_str(
                datetime_str=self.get_latest_metadata_update_datetime()
            )
            if latest_source_data_update > latest_pull:
                self.data_freshness_check["updated_data_available"] = True
            if latest_source_metadata_update > latest_pull:
                self.data_freshness_check["updated_metadata_available"] = True
            if (latest_pull >= latest_source_data_update) & (
                latest_pull >= latest_source_metadata_update
            ):
                self.data_freshness_check["data_pulled_this_check"] = False

    def get_this_tables_prior_freshness_checks_from_db(self, engine: Engine) -> pd.DataFrame:
        table_metadata_obj = get_reflected_db_table(
            engine=engine, table_name="table_metadata", schema_name="metadata"
        )
        select_query = select(table_metadata_obj).where(
            table_metadata_obj.c.table_id == self.data_freshness_check["table_id"]
        )
        return execute_result_returning_orm_query(engine=engine, select_query=select_query)

    def get_current_freshness_check_metadata_from_db(self, engine: Engine) -> pd.DataFrame:
        if self.data_freshness_check["updated_data_available"] is None:
            self.check_warehouse_data_freshness(engine=engine)
        prior_freshness_check_df = self.get_this_table_ids_prior_freshness_checks_from_db(
            engine=engine
        )
        return prior_freshness_check_df.loc[
            prior_freshness_check_df["time_of_check"] == self.data_freshness_check["time_of_check"]
        ].reset_index(drop=True)

    def format_file_name(self) -> str:
        return f"{self.table_id}_{self.time_of_check}.{self.download_format}"

    def insert_current_freshness_check_to_db(self, engine: Engine) -> None:
        if self.data_freshness_check["updated_data_available"] is None:
            self.check_warehouse_data_freshness(engine=engine)
        metadata_table = get_reflected_db_table(
            engine=engine, table_name="table_metadata", schema_name="metadata"
        )
        if self.freshness_check_id is None:
            insert_statement = (
                insert(metadata_table).values(self.data_freshness_check).returning(metadata_table)
            )
            result_df = execute_result_returning_query(engine=engine, query=insert_statement)
            if len(result_df) != 1:
                raise Exception("There should only be one result returned for this freshness check")
            self.freshness_check_id = result_df["id"].max()
        else:
            # I'd rather separate insert and update logic, and I don't know if I want to
            # throw an exception for this (although that may change).
            pass

    # def get_insertable_check_table_metadata_record(
    #     self, engine: Engine, output_type: str = "DataFrame"
    # ) -> Union[pd.DataFrame, Tuple]:
    #     output_type = output_type.lower()
    #     assert output_type in [
    #         "dataframe",
    #         "tuple",
    #     ], "Invalid 'output_type'; only [DataFrame or Tuple] are valid"
    #     if self.table_check_metadata["updated_data_available"] is None:
    #         self.check_table_metadata(engine=engine)
    #     if output_type == "tuple":
    #         return tuple(self.table_check_metadata.values())
    #     else:
    #         table_check_dict = self.table_check_metadata.copy()
    #         for key in table_check_dict.keys():
    #             table_check_dict[key] = [table_check_dict[key]]
    #         table_check_df = pd.DataFrame(table_check_dict)
    #         # table_check_df["metadata_json"] = table_check_df["metadata_json"].apply(json.dumps)
    #         return table_check_df

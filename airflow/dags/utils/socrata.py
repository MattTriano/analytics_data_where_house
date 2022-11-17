import datetime as dt
import json
import re
from pathlib import Path
from typing import Dict, Optional, Union

import pandas as pd
import requests
from sqlalchemy.engine.base import Engine

from db import execute_result_returning_query


class SocrataTableMetadata:
    def __init__(self, table_id: str, table_name: Optional[str] = None):
        self.table_id = table_id
        self.metadata = self.get_table_metadata()
        self.table_name = self.validate_table_name(table_name=table_name)
        self.resource_metadata = self.get_resource_metadata()
        self.column_details = self.get_column_details()
        self.has_geospatial_feature = self.table_has_geospatial_feature()
        self.data_domain = self.get_data_domain()
        self.is_geospatial = self.is_geospatial_table()

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

    def get_valid_geospatial_export_formats(self) -> Dict:
        valid_export_formats = {
            "shp": "Shapefile",
            "shapefile": "Shapefile",
            "geojson": "GeoJSON",
            "kmz": "KMZ",
            "kml": "KML",
        }
        return valid_export_formats

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

    def format_geospatial_export_format(self, export_format: str) -> str:
        valid_export_formats = self.get_valid_geospatial_export_formats()
        if export_format in valid_export_formats.values():
            return export_format
        else:
            if export_format.lower() not in valid_export_formats.keys():
                raise Exception("Invalid geospatial format")
            return valid_export_formats[export_format.lower()]

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

    def get_data_download_url(self, export_format: str = "GeoJSON") -> str:
        export_format = self.format_geospatial_export_format(export_format=export_format)
        if self.is_geospatial_table():
            return f"https://{self.data_domain}/api/geospatial/{self.table_id}?method=export&format={export_format}"
        else:
            return (
                f"https://{self.data_domain}/api/views/{self.table_id}/rows.csv?accessType=DOWNLOAD"
            )

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

    def get_table_metadata_check_template(self) -> Dict:
        return {
            "table_id": self.table_id,
            "table_name": self.table_name,
            "source_data_last_updated": self.get_latest_data_update_datetime(),
            "source_metadata_last_updated": self.get_latest_metadata_update_datetime(),
            "updated_data_available": None,
            "updated_metadata_available": None,
            "data_pulled_this_check": None,
            "time_of_check": self.metadata["time_of_collection"],
            "metadata_json": self.metadata,
        }

    def check_table_metadata(self, engine: Engine):
        check_df = self.get_prior_metadata_checks_from_db(engine=engine)
        table_check = self.get_table_metadata_check_template()
        table_check["updated_data_available"] = False
        table_check["updated_metadata_available"] = False
        if len(check_df) == 0:
            table_check["updated_data_available"] = True
            table_check["updated_metadata_available"] = True
        else:
            data_pull_mask = check_df["data_pulled_this_check"] == True
            latest_pull = check_df.loc[data_pull_mask, "time_of_check"].max()
            if self.get_latest_data_update_datetime() > latest_pull:
                table_check["updated_data_available"] = True
            if self.get_latest_metadata_update_datetime() > latest_pull:
                table_check["updated_metadata_available"] = True
        self.table_check

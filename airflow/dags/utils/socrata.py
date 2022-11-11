from pathlib import Path
import datetime as dt
import json
import re
from typing import Dict, Optional

import requests


def get_socrata_table_metadata(table_id: str) -> Dict:
    api_call = f"http://api.us.socrata.com/api/catalog/v1?ids={table_id}"
    response = requests.get(api_call)
    if response.status_code == 200:
        response_json = response.json()
        results = {"_id": table_id, "time_of_collection": dt.datetime.utcnow()}
        results.update(response_json["results"][0])
        return results


def dump_socrata_metadata_to_json(table_metadata: Dict, file_path: Path) -> None:
    with open(file_path, "w", encoding="utf-8") as json_file:
        json.dump(table_metadata, json_file, ensure_ascii=False, indent=4, default=str)


def get_table_metadata_attr(metadata_dict: Dict, attr_name: str) -> str:
    try:
        if attr_name in metadata_dict.keys():
            return metadata_dict[attr_name]
        else:
            return None
    except Exception as err:
        print(f"Exception {err} with type {type(err)} raised for attr_name '{attr_name}'.")
        print("Did you mean to enter metadata_dict['resource']")
        raise


def extract_column_details_from_table_metadata(table_metadata: Dict) -> Dict:
    try:
        resource_metadata = table_metadata["resource"]
        column_metadata_fields = [
            "columns_name",
            "columns_field_name",
            "columns_datatype",
            "columns_description",
            "columns_format",
        ]
        present_col_metadata_fields = [
            el for el in column_metadata_fields if el in resource_metadata.keys()
        ]
        col_json = {field: resource_metadata[field] for field in present_col_metadata_fields}
        return col_json
    except Exception as err:
        print(f"Exception {err} with type {type(err)} raised.")
        raise


def has_geospatial_feature(table_metadata: Dict) -> bool:
    socrata_geo_datatypes = [
        "Line",
        "Location",
        "MultiLine",
        "MultiPoint",
        "MultiPolygon",
        "Point",
        "Polygon",
    ]
    col_json = extract_column_details_from_table_metadata(table_metadata=table_metadata)
    if "columns_datatype" in col_json.keys():
        column_datatypes = col_json["columns_datatype"]
        table_has_geo_column = any(
            [col_dtype in socrata_geo_datatypes for col_dtype in column_datatypes]
        )
        return table_has_geo_column
    else:
        raise Exception("No 'columns_datatype' field found in table_metadata")


def validate_metadata_fields(table_metadata: Dict) -> Dict:
    try:
        resource_metadata = table_metadata["resource"]
        col_fields = [
            "name",
            "id",
            "description",
            "attribution",
            "type",
            "updatedAt",
            "createdAt",
            "metadata_updated_at",
            "data_updated_at",
            "provenance",
            "lens_view_type",
            "lens_display_type",
            "publication_date",
        ]
        validated_field_vals = {
            field: get_table_metadata_attr(metadata_dict=resource_metadata, attr_name=field)
            for field in col_fields
        }
        return validated_field_vals
    except Exception as err:
        print(f"Exception {err} with type {type(err)} raised.")
        raise


class SocrataTableMetadata:
    def __init__(self, table_id: str, table_name: Optional[str] = None):
        self.table_id = table_id
        self.set_table_metadata()
        self.set_table_name(table_name=table_name)

    def set_table_metadata(self) -> None:
        api_call = f"http://api.us.socrata.com/api/catalog/v1?ids={self.table_id}"
        response = requests.get(api_call)
        if response.status_code == 200:
            response_json = response.json()
            self.metadata = {"_id": self.table_id, "time_of_collection": dt.datetime.utcnow()}
            self.metadata.update(response_json["results"][0])
        else:
            raise Exception(
                f"Request for metadata for table {self.table_id} failed with status code {response.status_code}"
            )

    def get_table_metadata_attr(self, metadata_dict: dict, attr_name: str) -> str:
        try:
            if attr_name in metadata_dict.keys():
                return metadata_dict[attr_name]
            else:
                return None
        except Exception as err:
            print(f"Exception {err} with type {type(err)} raised for attr_name '{attr_name}'.")
            print("Did you mean to enter metadata_dict['resource']")
            raise

    def get_resource_metadata(self):
        return self.get_table_metadata_attr(metadata_dict=self.metadata, attr_name="resource")

    def set_table_name(self, table_name: Optional[str]) -> None:
        if table_name is None:
            resource_metadata = self.get_resource_metadata()
            table_name = self.get_table_metadata_attr(
                metadata_dict=resource_metadata, attr_name="name"
            )
            self.table_name = "_".join(re.sub("[^0-9a-zA-Z]+", "", table_name.lower()).split())
        else:
            self.table_name = table_name

    def dump_socrata_metadata_to_json(self, file_path: Path) -> None:
        with open(file_path, "w", encoding="utf-8") as json_file:
            json.dump(self.metadata, json_file, ensure_ascii=False, indent=4, default=str)

    def extract_column_details_from_table_metadata(self) -> Dict:
        try:
            resource_metadata = self.get_resource_metadata()
            column_metadata_fields = [
                "columns_name",
                "columns_field_name",
                "columns_datatype",
                "columns_description",
                "columns_format",
            ]
            present_col_metadata_fields = [
                el for el in column_metadata_fields if el in resource_metadata.keys()
            ]
            col_json = {field: resource_metadata[field] for field in present_col_metadata_fields}
            return col_json
        except Exception as err:
            print(f"Exception {err} with type {type(err)} raised.")
            raise

    def has_geospatial_feature(self) -> bool:
        socrata_geo_datatypes = [
            "Line",
            "Location",
            "MultiLine",
            "MultiPoint",
            "MultiPolygon",
            "Point",
            "Polygon",
        ]
        col_json = self.extract_column_details_from_table_metadata()
        if "columns_datatype" in col_json.keys():
            column_datatypes = col_json["columns_datatype"]
            table_has_geo_column = any(
                [col_dtype in socrata_geo_datatypes for col_dtype in column_datatypes]
            )
            return table_has_geo_column
        else:
            raise Exception("No 'columns_datatype' field found in table_metadata")

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
            metadata_dict=self.get_resource_metadata(), attr_name="lens_view_type"
        )
        return table_view_type == "geo"

    def table_has_map_type_display(self) -> bool:
        table_display_type = self.get_table_metadata_attr(
            metadata_dict=self.get_resource_metadata(), attr_name="lens_display_type"
        )
        return table_display_type == "map"

    def table_has_data_columns(self) -> bool:
        table_data_cols = self.get_table_metadata_attr(
            metadata_dict=self.get_resource_metadata(), attr_name="columns_name"
        )
        return len(table_data_cols) != 0

    def is_geospatial_table(self) -> bool:
        return (
            (not self.table_has_data_columns())
            and (self.table_has_geo_type_view() or self.table_has_map_type_display())
        ) or (self.table_has_geo_column())

    def format_geospatial_export_format(self, export_format: str) -> str:
        valid_export_formats = self.get_valid_geospatial_export_formats()
        if export_format in valid_export_formats.values():
            return export_format
        else:
            if export_format.lower() not in valid_export_formats.keys():
                raise Exception("Invalid geospatial format")
            return valid_export_formats[export_format.lower()]

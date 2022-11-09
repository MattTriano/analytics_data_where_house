from pathlib import Path
import datetime as dt
import json
from typing import Dict

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
            'columns_name', 'columns_field_name', 'columns_datatype', 'columns_description', 'columns_format'
        ]
        present_col_metadata_fields = [el for el in column_metadata_fields if el in resource_metadata.keys()]
        col_json = {field:resource_metadata[field] for field in present_col_metadata_fields}
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
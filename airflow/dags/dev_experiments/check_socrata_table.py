import datetime as dt
import json
from pathlib import Path
from typing import Dict

from airflow.decorators import dag, task
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


@dag(
    schedule=None,
    start_date=dt.datetime(2022, 11, 1),
    catchup=False,
    tags=['dev_experiment'],
)
def get_socrata_table():

    @task()
    def check_table_metadata(table_id: str = "pcdw-pxtg"):
        table_metadata_dict = get_socrata_table_metadata(table_id=table_id)
        out_path = Path(f"/opt/airflow/data_raw/table_{table_id}_metadata.json")
        dump_socrata_metadata_to_json(table_metadata=table_metadata_dict, file_path=out_path)
    
    check_table_metadata()
    
# get_socrata_table()
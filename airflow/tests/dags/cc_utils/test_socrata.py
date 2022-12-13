import json
from pathlib import Path
import unittest
import pytest
import sys
from typing import Dict
from unittest.mock import Mock, patch

sys.path.append("../../airflow")


from dags.cc_utils import socrata


def load_table_metadata_json(table_id: str) -> Dict:
    if "__file__" in globals().keys():
        file_path = (
            Path(__file__)
            .resolve()
            .parent.joinpath("data", f"sample_metadata_for_table_{table_id}.json")
        )
    else:
        raise Exception("run tests from the command line, please")
    if file_path.is_file():
        with open(file_path, "r", encoding="utf-8") as json_file:
            return json.load(json_file)
    else:
        raise Exception(f"No file found in location {file_path}")


@pytest.fixture(scope="session")
def monkeysession():
    with pytest.MonkeyPatch.context() as mp:
        yield mp


@pytest.fixture(scope="class")
def mock_SocrataTableMetadata(monkeysession):
    def mock_get_table_metadata(socrata_table):
        print(f"socrata_table: {socrata_table}")
        return load_table_metadata_json(table_id=socrata_table.table_id)

    monkeysession.setattr(
        socrata.SocrataTableMetadata, "get_table_metadata", mock_get_table_metadata
    )


class TestCSVSocrataTableMetadata:
    @pytest.fixture(scope="class")
    def socrata_metadata(self, mock_SocrataTableMetadata):
        socrata_table = socrata.SocrataTable(
            table_id="gumc-mgzr", table_name="chicago_homicide_and_shooting_victimizations"
        )
        mock_socrata_metadata = socrata.SocrataTableMetadata(socrata_table=socrata_table)
        yield mock_socrata_metadata

    def test_time_of_collection_again(self, socrata_metadata):
        print(f"socrata_metadata: {socrata_metadata}")
        assert socrata_metadata.metadata["time_of_collection"] == "2022-12-11T21:11:43.525120Z"


class TestGeojsonSocrataTableMetadata:
    @pytest.fixture(scope="class")
    def socrata_metadata(self, mock_SocrataTableMetadata):
        socrata_table = socrata.SocrataTable(
            table_id="wyzt-dzf8", table_name="cook_county_neighborhood_boundaries"
        )
        mock_socrata_metadata = socrata.SocrataTableMetadata(socrata_table=socrata_table)
        yield mock_socrata_metadata

    def test_time_of_collection_again(self, socrata_metadata):
        print(f"socrata_metadata: {socrata_metadata}")
        assert socrata_metadata.metadata["time_of_collection"] == "2022-12-12T15:47:47.359187Z"

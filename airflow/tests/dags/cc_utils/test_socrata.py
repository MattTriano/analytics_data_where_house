import json
import logging
from pathlib import Path
import sys
from typing import Dict
import unittest
from unittest.mock import Mock, patch

import pytest

sys.path.append("../../airflow")

from dags.cc_utils import socrata

LOGGER = logging.getLogger("SocrataTesting")


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
            table_id="wvhk-k5uv", table_name="cook_county_parcel_sales"
        )
        mock_socrata_metadata = socrata.SocrataTableMetadata(socrata_table=socrata_table)
        yield mock_socrata_metadata

    def test_time_of_collection(self, socrata_metadata):
        assert socrata_metadata.metadata["time_of_collection"] == "2022-12-13T04:44:51.717900Z"

    def test_is_geospatial(self, socrata_metadata):
        LOGGER.info(f"socrata_metadata.table_name: {socrata_metadata.table_name}")
        LOGGER.info(f"socrata_metadata.data_domain: {socrata_metadata.data_domain}")
        LOGGER.info(f"socrata_metadata.get_column_details: {socrata_metadata.get_column_details()}")
        assert socrata_metadata.is_geospatial == False

    def test_has_a_geospatial_feature(self, socrata_metadata):
        assert socrata_metadata.table_has_geospatial_feature() == False

    def test_has_geo_type_view(self, socrata_metadata):
        assert socrata_metadata.table_has_geo_type_view() == False

    def test_has_map_type_display(self, socrata_metadata):
        assert socrata_metadata.table_has_map_type_display() == False

    def test_has_data_columns(self, socrata_metadata):
        assert socrata_metadata.table_has_data_columns() == True


class TestGeojsonSocrataTableMetadata:
    @pytest.fixture(scope="class")
    def socrata_metadata(self, mock_SocrataTableMetadata):
        socrata_table = socrata.SocrataTable(
            table_id="wyzt-dzf8", table_name="cook_county_neighborhood_boundaries"
        )
        mock_socrata_metadata = socrata.SocrataTableMetadata(socrata_table=socrata_table)
        yield mock_socrata_metadata

    def test_time_of_collection(self, socrata_metadata):
        assert socrata_metadata.metadata["time_of_collection"] == "2022-12-12T15:47:47.359187Z"

    def test_is_geospatial(self, socrata_metadata):
        LOGGER.info(f"socrata_metadata.table_name: {socrata_metadata.table_name}")
        LOGGER.info(f"socrata_metadata.data_domain: {socrata_metadata.data_domain}")
        LOGGER.info(f"socrata_metadata.get_column_details: {socrata_metadata.get_column_details()}")
        assert socrata_metadata.is_geospatial == True

    def test_has_a_geospatial_feature(self, socrata_metadata):
        assert socrata_metadata.table_has_geospatial_feature() == True

    def test_has_geo_type_view(self, socrata_metadata):
        assert socrata_metadata.table_has_geo_type_view() == False

    def test_has_map_type_display(self, socrata_metadata):
        assert socrata_metadata.table_has_map_type_display() == False

    def test_has_data_columns(self, socrata_metadata):
        assert socrata_metadata.table_has_data_columns() == True

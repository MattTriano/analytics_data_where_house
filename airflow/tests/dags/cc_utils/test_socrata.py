import json
import logging
from pathlib import Path
import sys
from typing import Dict
import unittest
from unittest.mock import Mock, patch

from inspect import getsource

import pandas as pd
import pytest

sys.path.append("../../airflow")

from dags.cc_utils import socrata
from dags.sources.tables import (
    CHICAGO_CITY_BOUNDARY,
    COOK_COUNTY_PARCEL_SALES,
    COOK_COUNTY_NEIGHBORHOOD_BOUNDARIES,
)

LOGGER = logging.getLogger("SocrataTesting")


def load_json(file_path: Path) -> Dict:
    if file_path.is_file():
        with open(file_path, "r", encoding="utf-8") as json_file:
            return json.load(json_file)
    else:
        raise Exception(f"No file found in location {file_path}")


def load_table_metadata_json(table_id: str) -> Dict:
    if "__file__" in globals().keys():
        file_path = (
            Path(__file__)
            .resolve()
            .parent.joinpath("data", f"sample_metadata_for_table_{table_id}.json")
        )
    else:
        raise Exception("run tests from the command line, please")
    return load_json(file_path)


@pytest.fixture(scope="session")
def monkeysession():
    with pytest.MonkeyPatch.context() as mp:
        yield mp


@pytest.fixture(scope="class")
def mock_SocrataTableMetadata(monkeysession, table_metadata_df):
    def mock_get_table_metadata(socrata_table):
        return load_table_metadata_json(table_id=socrata_table.table_id)

    def mock_get_prior_metadata_checks_from_db(*args, **kwargs) -> pd.DataFrame:
        check_df = table_metadata_df.loc[
            table_metadata_df["table_id"] == locals()["kwargs"]["engine"].table_id
        ].copy()
        return check_df

    monkeysession.setattr(
        socrata.SocrataTableMetadata, "get_table_metadata", mock_get_table_metadata
    )
    monkeysession.setattr(
        socrata.SocrataTableMetadata,
        "get_prior_metadata_checks_from_db",
        mock_get_prior_metadata_checks_from_db,
    )


class TestFreshnessCheckLogic:
    @pytest.fixture(scope="class")
    def socrata_metadata_reg_updates(self, mock_SocrataTableMetadata):
        socrata_table = COOK_COUNTY_PARCEL_SALES
        mock_socrata_metadata = socrata.SocrataTableMetadata(socrata_table=socrata_table)
        mock_socrata_metadata.initialize_data_freshness_check_record()
        yield mock_socrata_metadata

    @pytest.fixture(scope="class")
    def socrata_metadata_never_updated(self, mock_SocrataTableMetadata):
        socrata_table = CHICAGO_CITY_BOUNDARY
        mock_socrata_metadata = socrata.SocrataTableMetadata(socrata_table=socrata_table)
        mock_socrata_metadata.initialize_data_freshness_check_record()
        yield mock_socrata_metadata

    def test_reg_updates_freshness_check_logic(self, socrata_metadata_reg_updates):
        socrata_metadata_reg_updates.check_warehouse_data_freshness(
            engine=socrata_metadata_reg_updates
        )
        assert socrata_metadata_reg_updates.latest_data_update_datetime == "2022-12-01T06:16:57Z"
        assert socrata_metadata_reg_updates.data_freshness_check["data_pulled_this_check"] is None
        assert socrata_metadata_reg_updates.data_freshness_check["updated_data_available"] == True

    def test_never_updated_freshness_check_logic(self, socrata_metadata_never_updated):
        socrata_metadata_never_updated.check_warehouse_data_freshness(
            engine=socrata_metadata_never_updated
        )
        assert socrata_metadata_never_updated.latest_data_update_datetime is None
        assert (
            socrata_metadata_never_updated.data_freshness_check["data_pulled_this_check"] == False
        )
        assert (
            socrata_metadata_never_updated.data_freshness_check["updated_data_available"] == False
        )


class TestCSVSocrataTableMetadata:
    @pytest.fixture(scope="class")
    def socrata_metadata(self, mock_SocrataTableMetadata):
        socrata_table = COOK_COUNTY_PARCEL_SALES
        mock_socrata_metadata = socrata.SocrataTableMetadata(socrata_table=socrata_table)
        yield mock_socrata_metadata

    def test_time_of_collection(self, socrata_metadata):
        assert socrata_metadata.metadata["time_of_collection"] == "2022-12-13T04:44:51.717900Z"

    def test_is_geospatial(self, socrata_metadata):
        assert socrata_metadata.is_geospatial == False

    def test_has_a_geospatial_feature(self, socrata_metadata):
        assert socrata_metadata.table_has_geospatial_feature == False

    def test_has_geo_type_view(self, socrata_metadata):
        assert socrata_metadata.table_has_geo_type_view == False

    def test_has_map_type_display(self, socrata_metadata):
        assert socrata_metadata.table_has_map_type_display == False

    def test_has_data_columns(self, socrata_metadata):
        assert socrata_metadata.table_has_data_columns == True

    def test_data_domain(self, socrata_metadata):
        assert socrata_metadata.data_domain == "datacatalog.cookcountyil.gov"

    def test_table_name(self, socrata_metadata):
        assert socrata_metadata.table_name == "cook_county_parcel_sales"

    def test_data_download_url(self, socrata_metadata):
        assert socrata_metadata.data_download_url == (
            "https://datacatalog.cookcountyil.gov/api/views/wvhk-k5uv/rows.csv?accessType=DOWNLOAD"
        )

    def test_download_format(self, socrata_metadata):
        assert socrata_metadata.download_format == "csv"

    def test_latest_data_update_datetime(self, socrata_metadata):
        assert socrata_metadata.latest_data_update_datetime == "2022-12-01T06:16:57Z"

    def test_latest_metadata_update_datetime(self, socrata_metadata):
        assert socrata_metadata.latest_metadata_update_datetime == "2022-12-01T06:11:38Z"


class TestGeojsonSocrataTableMetadata:
    @pytest.fixture(scope="class")
    def socrata_metadata(self, mock_SocrataTableMetadata):
        socrata_table = COOK_COUNTY_NEIGHBORHOOD_BOUNDARIES
        mock_socrata_metadata = socrata.SocrataTableMetadata(socrata_table=socrata_table)
        yield mock_socrata_metadata

    def test_time_of_collection(self, socrata_metadata):
        assert socrata_metadata.metadata["time_of_collection"] == "2022-12-12T15:47:47.359187Z"

    def test_is_geospatial(self, socrata_metadata):
        assert socrata_metadata.is_geospatial == True

    def test_has_a_geospatial_feature(self, socrata_metadata):
        assert socrata_metadata.table_has_geospatial_feature == True

    def test_has_geo_type_view(self, socrata_metadata):
        assert socrata_metadata.table_has_geo_type_view == False

    def test_has_map_type_display(self, socrata_metadata):
        assert socrata_metadata.table_has_map_type_display == False

    def test_has_data_columns(self, socrata_metadata):
        assert socrata_metadata.table_has_data_columns == True

    def test_data_domain(self, socrata_metadata):
        assert socrata_metadata.data_domain == "datacatalog.cookcountyil.gov"

    def test_table_name(self, socrata_metadata):
        assert socrata_metadata.table_name == "cook_county_neighborhood_boundaries"

    def test_data_download_url(self, socrata_metadata):
        assert socrata_metadata.data_download_url == (
            "https://datacatalog.cookcountyil.gov/api/geospatial/wyzt-dzf8?method=export"
            + "&format=GeoJSON"
        )

    def test_download_format(self, socrata_metadata):
        assert socrata_metadata.download_format == "GeoJSON"

    def test_latest_data_update_datetime(self, socrata_metadata):
        assert socrata_metadata.latest_data_update_datetime == "2020-05-21T15:38:11Z"

    def test_latest_metadata_update_datetime(self, socrata_metadata):
        assert socrata_metadata.latest_metadata_update_datetime == "2022-05-11T15:59:20Z"


class TestGeojsonMapTypeSocrataTableMetadata:
    @pytest.fixture(scope="class")
    def socrata_metadata(self, mock_SocrataTableMetadata):
        socrata_table = CHICAGO_CITY_BOUNDARY
        mock_socrata_metadata = socrata.SocrataTableMetadata(socrata_table=socrata_table)
        yield mock_socrata_metadata

    def test_time_of_collection(self, socrata_metadata):
        assert socrata_metadata.metadata["time_of_collection"] == "2022-12-27T03:35:06.025250Z"

    def test_is_geospatial(self, socrata_metadata):
        assert socrata_metadata.is_geospatial == True

    def test_has_a_geospatial_feature(self, socrata_metadata):
        assert socrata_metadata.table_has_geospatial_feature == False

    def test_has_geo_type_view(self, socrata_metadata):
        assert socrata_metadata.table_has_geo_type_view == True

    def test_has_map_type_display(self, socrata_metadata):
        assert socrata_metadata.table_has_map_type_display == True

    def test_has_data_columns(self, socrata_metadata):
        assert socrata_metadata.table_has_data_columns == False

    def test_data_domain(self, socrata_metadata):
        assert socrata_metadata.data_domain == "data.cityofchicago.org"

    def test_table_name(self, socrata_metadata):
        assert socrata_metadata.table_name == "chicago_city_boundary"

    def test_data_download_url(self, socrata_metadata):
        assert socrata_metadata.data_download_url == (
            "https://data.cityofchicago.org/api/geospatial/ewy2-6yfk?method=export"
            + "&format=GeoJSON"
        )

    def test_download_format(self, socrata_metadata):
        assert socrata_metadata.download_format == "GeoJSON"

    def test_latest_data_update_datetime(self, socrata_metadata):
        assert socrata_metadata.latest_data_update_datetime == None

    def test_latest_metadata_update_datetime(self, socrata_metadata):
        assert socrata_metadata.latest_metadata_update_datetime == "2017-06-30T22:02:43Z"

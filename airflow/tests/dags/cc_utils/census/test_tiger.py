from pathlib import Path
from typing import Dict
from unittest.mock import patch

import json
import pandas as pd
import pytest

from cc_utils.census.api import CensusGeogTract
from cc_utils.census.tiger import (
    TIGERCatalog,
    TIGERVintageCatalog,
    TIGERGeographicEntityVintage,
)

FILE_DIR = Path(__file__).resolve().parent


def load_census_files_metadata(fp):
    df = pd.read_json(fp)
    df["last_modified"] = pd.to_datetime(df["last_modified"])
    return df


@pytest.fixture(scope="module")
def tiger_catalog():
    with patch(
        "cc_utils.census.tiger.get_tiger_vintages_metadata",
        new=lambda: load_census_files_metadata(
            fp=FILE_DIR.joinpath("data", "tiger_catalog_metadata.json")
        ),
    ):
        return TIGERCatalog()


@pytest.fixture(scope="module")
def vintage_catalog(tiger_catalog):
    with patch.object(
        TIGERVintageCatalog,
        "vintage_entities",
        new=lambda: load_census_files_metadata(
            fp=FILE_DIR.joinpath("data", "tiger_vintage_catalog_metadata.json")
        ),
    ):
        return TIGERVintageCatalog(year="2022", catalog=tiger_catalog)


@pytest.fixture(scope="module")
def geographic_entity_vintage(vintage_catalog):
    with patch(
        "cc_utils.census.tiger.TIGERVintageCatalog",
        return_value=vintage_catalog,
    ), patch.object(
        TIGERGeographicEntityVintage,
        "entity_files_metadata",
        new=lambda: load_census_files_metadata(
            fp=FILE_DIR.joinpath("data", "geographic_entity_(tract)_metadata.json")
        ),
    ):
        return TIGERGeographicEntityVintage(entity_name="TRACT", year="2022")


def test_TIGERCatalog_get_vintage_metadata(tiger_catalog):
    vintage_metadata = tiger_catalog.get_vintage_metadata(year="2022")
    assert vintage_metadata["last_modified"].values[0] > pd.to_datetime("2022-10-30")
    assert vintage_metadata["last_modified"].values[0] < pd.to_datetime("2022-11-02")
    assert vintage_metadata["is_dir"].values[0] == True
    assert vintage_metadata["clean_name"].values[0] == vintage_metadata["name"].values[0].replace(
        "/", ""
    )
    assert vintage_metadata["clean_name"].values[0] == "TIGER2022"
    assert (
        vintage_metadata["metadata_url"].values[0] == "https://www2.census.gov/geo/tiger/TIGER2022"
    )


def test_TIGERVintageCatalog_get_entity_metadata(vintage_catalog):
    entity_metadata = vintage_catalog.get_entity_metadata(entity_name="TRACT")
    assert entity_metadata["last_modified"].values[0] > pd.to_datetime("2022-09-30")
    assert entity_metadata["last_modified"].values[0] < pd.to_datetime("2022-10-01")
    assert entity_metadata["is_dir"].values[0] == True
    assert entity_metadata["clean_name"].values[0] == entity_metadata["name"].values[0].replace(
        "/", ""
    )
    assert (
        entity_metadata["metadata_url"].values[0]
        == "https://www2.census.gov/geo/tiger/TIGER2022/TRACT"
    )


def test_TIGERGeographicEntityVintage_entity_url(geographic_entity_vintage):
    assert (
        geographic_entity_vintage.entity_url == "https://www2.census.gov/geo/tiger/TIGER2022/TRACT"
    )


def test_TIGERGeographicEntityVintage_entity_files_metadata(geographic_entity_vintage):
    assert len(geographic_entity_vintage.entity_files_metadata) == 56


def test_TIGERGeographicEntityVintage_get_geometry_filter_tracts_in_a_single_county(
    geographic_entity_vintage,
):
    filter_str = geographic_entity_vintage.get_geometry_filter_str(
        geography=CensusGeogTract(state_cd="17", county_cd="031")
    )
    assert filter_str == "_17_"


def test_TIGERGeographicEntityVintage_get_geometry_filter_tracts_in_multiple_states(
    geographic_entity_vintage,
):
    geog_object = CensusGeogTract(state_cd=["17", "18", "19"], county_cd="031")
    filter_str = geographic_entity_vintage.get_geometry_filter_str(geography=geog_object)
    assert filter_str == "_17*_|_18*_|_19*_"
    assert geog_object.state_cd == ["17", "18", "19"]

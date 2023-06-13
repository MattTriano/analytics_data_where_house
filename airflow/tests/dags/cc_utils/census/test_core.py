from pathlib import Path
from typing import Dict

import json
import pandas as pd

from cc_utils.census.core import (
    get_dataset_groups_metadata,
    get_dataset_tags_metadata,
)


def read_file(file_path: Path):
    with open(file_path, "r") as f:
        data = json.load(f)
    return data


def mock_get_url_response(url: Dict) -> Dict:
    return url


def test_get_dataset_groups_metadata_when_no_groups(mocker):
    mocker.patch(
        "cc_utils.census.core.get_url_response", side_effect=lambda url: mock_get_url_response(url)
    )
    groups_df = get_dataset_groups_metadata(groups_url={"groups": []})
    assert groups_df.equals(
        pd.DataFrame(
            {
                "group_name": ["no_groups"],
                "group_description": ["no_groups"],
                "group_variables": ["no_groups"],
                "universe": ["no_groups"],
            }
        )
    )


def test_get_dataset_groups_metadata_with_a_valid_group(mocker):
    mocker.patch("cc_utils.census.core.get_url_response", side_effect=lambda url: read_file(url))
    groups_df = get_dataset_groups_metadata(
        groups_url=Path(".")
        .resolve()
        .joinpath("tests/dags/cc_utils/census/data/valid_groups_resp.json")
    )
    assert groups_df.equals(
        pd.DataFrame(
            {
                "group_name": ["AB2000CSCBO"],
                "group_description": [
                    "Annual Business Survey: Owner Characteristics of Respondent Employer Firms by Sector, Sex, Ethnicity, Race, and Veteran Status for the U.S., States, and Metro Areas: 2020"
                ],
                "group_variables": [
                    "http://api.census.gov/data/2020/abscbo/groups/AB2000CSCBO.json"
                ],
            }
        )
    )


def test_get_dataset_tags_metadata_when_null_tags_url(mocker):
    mocker.patch(
        "cc_utils.census.core.get_url_response", side_effect=lambda url: mock_get_url_response(url)
    )
    tags_df = get_dataset_tags_metadata(tags_url=None)
    assert tags_df.equals(pd.DataFrame({"tag_name": ["no_tags"]}))


def test_get_dataset_tags_metadata_with_valid_tags(mocker):
    mocker.patch("cc_utils.census.core.get_url_response", side_effect=lambda url: read_file(url))
    tags_df = get_dataset_tags_metadata(
        tags_url=Path(".")
        .resolve()
        .joinpath("tests/dags/cc_utils/census/data/valid_tags_resp.json")
    )
    assert tags_df.equals(pd.DataFrame({"tag_name": ["ethnicity", "race", "sex", "veteran"]}))

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


def test_get_dataset_tags_metadata_when_null_tags_url(mocker):
    mocker.patch(
        "cc_utils.census.core.get_url_response", side_effect=lambda url: mock_get_url_response(url)
    )
    tags_df = get_dataset_tags_metadata(tags_url=None)
    assert tags_df.equals(pd.DataFrame({"tag_name": ["no_tags"]}))

import logging
from pathlib import Path
import sys

import pandas as pd
import pytest

sys.path.append("../../airflow")

from dags.cc_utils.cleanup import standardize_columns_fix_defect

LOGGER = logging.getLogger("UtilTesting")


@pytest.fixture(scope="function")
def some_invalid_col_names_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=["address", "EASE-MENT", "BUILDING CLASS CATEGORY", "BUILDING_CLASS_CATEGORY"]
    )


def test_fix_column_name_w_hyphen_defect(some_invalid_col_names_df):
    df = standardize_columns_fix_defect(df=some_invalid_col_names_df, defect_char="-")
    expected_columns = [
        "address",
        "ease_ment",
        "building class category",
        "building_class_category",
    ]
    assert all(df.columns == expected_columns)


def test_fix_column_name_w_spaces_defect(some_invalid_col_names_df):
    df = standardize_columns_fix_defect(df=some_invalid_col_names_df, defect_char=" ")
    expected_columns = [
        "address",
        "ease-ment",
        "building_class_category_1",
        "building_class_category",
    ]
    assert all(df.columns == expected_columns)

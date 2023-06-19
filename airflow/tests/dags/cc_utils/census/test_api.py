import pytest

from cc_utils.census.api import CensusGeogTract, CensusGeogBlockGroup, CensusGeogCounty


def test_create_CensusGeogBlockGroup_multiple_counties():
    block_groups_obj = CensusGeogBlockGroup(state_cd="17", county_cd=["031", "043"])
    assert (
        block_groups_obj.api_call_geographies
        == "for=block%20group:*&in=state:17&in=county:031,043&in=tract:*"
    )


def test_create_CensusGeogBlockGroup_single_county():
    block_groups_obj = CensusGeogBlockGroup(state_cd="17", county_cd="031")
    assert (
        block_groups_obj.api_call_geographies
        == "for=block%20group:*&in=state:17&in=county:031&in=tract:*"
    )


def test_create_CensusGeogTract_multiple_counties():
    tract_obj = CensusGeogTract(state_cd="17", county_cd=["031", "043"])
    assert tract_obj.api_call_geographies == "for=tract:*&in=state:17&in=county:031,043"


def test_create_CensusGeogTract_single_county():
    tract_obj = CensusGeogTract(state_cd="17", county_cd="031")
    assert tract_obj.api_call_geographies == "for=tract:*&in=state:17&in=county:031"


def test_create_CensusGeogCounty_multi_state_all_counties():
    county_obj = CensusGeogCounty(state_cd=["17", "18"], county_cd="*")
    assert county_obj.api_call_geographies == "for=county:*&in=state:17,18"


def test_create_CensusGeogCounty_multi_state_specific_counties_raises_ValueError():
    with pytest.raises(ValueError):
        _ = CensusGeogCounty(state_cd=["17", "18"], county_cd="031")


def test_create_CensusGeogCounty_single_state_all_counties():
    county_obj = CensusGeogCounty(state_cd="17", county_cd="*")
    assert county_obj.api_call_geographies == "for=county:*&in=state:17"


def test_create_CensusGeogCounty_single_state_one_county():
    county_obj = CensusGeogCounty(state_cd="17", county_cd="031")
    assert county_obj.api_call_geographies == "for=county:031&in=state:17"

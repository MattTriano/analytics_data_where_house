from cc_utils.census.api import CensusGeogTract, CensusGeogBlockGroup


def test_create_CensusGeogBlockGroup_multiple_counties():
    block_groups_obj = CensusGeogBlockGroup(state_cd="17", county_cd=["031", "043"])
    assert (
        block_groups_obj.api_call_geographies
        == "for=tract:*&in=state:17&in=county:031,043&in=tract:*"
    )

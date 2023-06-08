from typing import Dict

import pandas as pd
import re
import requests


def get_url_response(url: str) -> Dict:
    api_call = re.sub("\.html$", ".json", url)
    resp = requests.get(api_call)
    if resp.status_code == 200:
        resp_json = resp.json()
        return resp_json
    else:
        print(f"Failed to get a valid response; status code: {resp.status_code}")
        return None


def get_dataset_metadata_catalog(dataset_base_url: str) -> pd.DataFrame:
    catalog_resp_json = get_url_response(url=dataset_base_url)
    if catalog_resp_json is None:
        raise Exception(
            f"Request for metadata catalog for the dataset with url\n\n  {dataset_base_url}\n\n"
            + "failed to get a valid response."
        )
    catalog_resp_df = pd.json_normalize(catalog_resp_json)
    catalog_cols = [col for col in catalog_resp_df.columns if col != "dataset"]
    catalog_df = catalog_resp_df[catalog_cols].copy()
    datasets = catalog_resp_df["dataset"].iloc[0].copy()
    catalog_colname_fixes = {
        "@context": "metadata_context",
        "@id": "metadata_catalog_id",
        "@type": "metadata_type",
        "conformsTo": "conforms_to_schema",
        "describedBy": "data_schema_dictionary",
    }
    catalog_df = catalog_df.rename(columns=catalog_colname_fixes)
    print(f"Elements in Census data catalog datasets attr: {len(datasets)} ")
    df_list = []
    df_shape_list = []
    for dataset in datasets:
        df = pd.json_normalize(dataset)
        df_list.append(df)
        df_shape_list.append(df.shape)
    datasets_df = pd.concat(df_list)
    datasets_df = datasets_df.reset_index(drop=True)
    datasets_df["modified"] = pd.to_datetime(datasets_df["modified"])
    distribution_df = pd.json_normalize(datasets_df["distribution"].str[0])
    distribution_df.columns = [f"distribution_{col}" for col in distribution_df.columns]
    datasets_df = pd.merge(
        left=datasets_df,
        right=distribution_df,
        how="left",
        left_index=True,
        right_index=True,
    )
    datasets_df = datasets_df.sort_values(by="modified", ascending=False, ignore_index=True)

    datasets_df["join_col"] = 1
    catalog_df["join_col"] = 1
    full_df = pd.merge(left=datasets_df, right=catalog_df, how="inner", on="join_col")
    full_df = full_df.drop(columns=["join_col"])
    colname_fixes = {
        "distribution_accessURL": "dataset_base_url",
        "identifier": "identifier",
        "title": "title",
        "description": "description",
        "modified": "modified",
        "c_vintage": "vintage",
        "c_geographyLink": "geography_link",
        "c_variablesLink": "variables_link",
        "c_tagsLink": "tags_link",
        "c_examplesLink": "examples_link",
        "c_groupsLink": "groups_link",
        "c_sorts_url": "sorts_url",
        "c_dataset": "dataset",
        "spatial": "spatial",
        "temporal": "temporal",
        "bureauCode": "bureau_code",
        "programCode": "program_code",
        "keyword": "keyword",
        "c_isMicrodata": "is_microdata",
        "c_isAggregate": "is_aggregate",
        "c_isCube": "is_cube",
        "c_isAvailable": "is_available",
        "c_isTimeseries": "is_timeseries",
        "accessLevel": "access_level",
        "license": "license",
        "@type": "dataset_type",
        "contactPoint.fn": "contact_point_fn",
        "contactPoint.hasEmail": "contact_point_email",
        "references": "reference_docs",
        "c_documentationLink": "documentation_link",
        "distribution_@type": "distribution_type",
        "distribution_mediaType": "distribution_media_type",
        "distribution_description": "distribution_description",
        "distribution_format": "distribution_format",
        "distribution_title": "distribution_title",
        "distribution": "distribution",
        "publisher.name": "publisher_name",
        "publisher.@type": "publisher_type",
        "publisher.subOrganizationOf.@type": "publisher_suborg_of_type",
        "publisher.subOrganizationOf.name": "publisher_suborg_of_name",
        "publisher.subOrganizationOf.subOrganizationOf.@type": "publisher_suborg_of_suborg_of_type",
        "publisher.subOrganizationOf.subOrganizationOf.name": "publisher_suborg_of_suborg_of_name",
    }
    colname_fixes.update({v: v for k, v in catalog_colname_fixes.items()})
    full_df_cols = full_df.columns
    col_order = [col for col in colname_fixes.keys() if col in full_df_cols]
    full_df = full_df[col_order].copy()
    full_df = full_df.rename(columns=colname_fixes)
    for bool_col in ["is_microdata", "is_aggregate", "is_cube", "is_available", "is_timeseries"]:
        if bool_col in full_df.columns:
            full_df[bool_col] = full_df[bool_col].fillna(False)
    return full_df


def get_dataset_variables_metadata(variables_url: str) -> pd.DataFrame:
    variables_json = get_url_response(url=variables_url)
    if variables_json is None:
        raise Exception(
            f"Request for dataset variables metadata for the dataset with url\n\n"
            + f"  {variables_url}\n\n failed to get a valid response."
        )
    variables_df = pd.DataFrame(variables_json["variables"]).T
    variables_df.index.name = "variable"
    variables_df = variables_df.reset_index()
    var_col_namemap = {
        "variable": "variable",
        "label": "label",
        "concept": "concept",
        "predicateType": "predicate_type",
        "group": "dataset_group",
        "limit": "limit_call",
        "predicateOnly": "predicate_only",
        "hasGeoCollectionSupport": "has_geo_collection_support",
        "attributes": "attributes",
        "required": "required",
        "values": "values",
        "datetime": "datetime",
        "is-weight": "is_weight",
        "suggested-weight": "suggested_weight",
    }
    variables_df = variables_df.rename(columns=var_col_namemap)
    if "values" in variables_df.columns:
        variables_df["values"] = variables_df["values"].fillna({})
    if "predicate_only" in variables_df.columns:
        variables_df["predicate_only"] = variables_df["predicate_only"].fillna(False)
    if "has_geo_collection_support" in variables_df.columns:
        variables_df["has_geo_collection_support"] = variables_df[
            "has_geo_collection_support"
        ].fillna(False)
    if "is_weight" in variables_df.columns:
        variables_df["is_weight"] = variables_df["is_weight"].fillna(False)
    col_order = [col for col in var_col_namemap.values() if col in variables_df.columns]
    return variables_df[col_order].copy()


def get_dataset_geography_metadata(geog_url: str) -> pd.DataFrame:
    geo_resp_json = get_url_response(url=geog_url)
    geographies_df = pd.DataFrame(geo_resp_json["fips"])
    geo_col_namemap = {
        "name": "name",
        "geoLevelDisplay": "geo_level",
        "referenceDate": "reference_date",
        "requires": "requires",
        "wildcard": "wildcard",
        "optionalWithWCFor": "optional_with_wildcard_for",
    }
    geographies_df = geographies_df.rename(columns=geo_col_namemap)
    if "requires" in geographies_df.columns:
        geographies_df["requires"] = geographies_df["requires"].apply(
            lambda x: x if isinstance(x, list) else []
        )
    if "wildcard" in geographies_df.columns:
        geographies_df["wildcard"] = geographies_df["wildcard"].apply(
            lambda x: x if isinstance(x, list) else []
        )
    return geographies_df


def get_dataset_groups_metadata(groups_url: str) -> pd.DataFrame:
    groups_json = get_url_response(url=groups_url)
    group_col_namemap = {
        "name": "group_name",
        "description": "group_description",
        "variables": "group_variables",
        "universe": "universe",
    }
    if groups_json is None:
        groups_df = pd.DataFrame({v: [None] for v in group_col_namemap.values()})
    else:
        groups_df = pd.DataFrame(groups_json["groups"])
        groups_df.columns = [col.strip() for col in groups_df.columns]
        groups_df = groups_df.rename(columns=group_col_namemap)
    return groups_df


def get_dataset_tags_metadata(tags_url: str) -> pd.DataFrame:
    tags_json = get_url_response(url=tags_url)
    tag_col_namemap = {"tags": "tag_name"}
    if tags_json is not None:
        tags_df = pd.DataFrame(tags_json)
        tags_df = tags_df.rename(columns=tag_col_namemap)
        return tags_df
    else:
        return pd.DataFrame({"tags": None})

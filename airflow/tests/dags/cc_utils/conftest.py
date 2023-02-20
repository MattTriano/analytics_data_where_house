import pandas as pd
import pytest


@pytest.fixture(scope="module")
def table_metadata_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "id": 220,
                "table_id": "ewy2-6yfk",
                "table_name": "chicago_city_boundary",
                "download_format": "GeoJSON",
                "is_geospatial": True,
                "data_download_url": "https://data.cityofchicago.org/api/geospatial/ewy2-6yfk?method=export&format=GeoJSON",
                "source_data_last_updated": None,
                "source_metadata_last_updated": pd.Timestamp("2017-06-30 22:02:43+0000", tz="UTC"),
                "updated_data_available": True,
                "updated_metadata_available": True,
                "data_pulled_this_check": True,
                "time_of_check": pd.Timestamp("2022-12-27 02:32:07.707954+0000", tz="UTC"),
                "metadata_json": {},
            },
            {
                "id": 6,
                "table_id": "wvhk-k5uv",
                "table_name": "cook_county_parcel_sales",
                "download_format": "csv",
                "is_geospatial": False,
                "data_download_url": "https://datacatalog.cookcountyil.gov/api/views/wvhk-k5uv/rows.csv?accessType=DOWNLOAD",
                "source_data_last_updated": pd.Timestamp("2022-11-01 05:26:43+0000", tz="UTC"),
                "source_metadata_last_updated": pd.Timestamp("2022-11-01 05:16:37+0000", tz="UTC"),
                "updated_data_available": True,
                "updated_metadata_available": True,
                "data_pulled_this_check": True,
                "time_of_check": pd.Timestamp("2022-11-22 05:18:10.480267+0000", tz="UTC"),
                "metadata_json": {},
            },
            {
                "id": 78,
                "table_id": "wyzt-dzf8",
                "table_name": "cook_county_neighborhood_boundaries",
                "download_format": "GeoJSON",
                "is_geospatial": True,
                "data_download_url": "https://datacatalog.cookcountyil.gov/api/geospatial/wyzt-dzf8?method=export&format=GeoJSON",
                "source_data_last_updated": pd.Timestamp("2020-05-21 15:38:11+0000", tz="UTC"),
                "source_metadata_last_updated": pd.Timestamp("2022-05-11 15:59:20+0000", tz="UTC"),
                "updated_data_available": True,
                "updated_metadata_available": True,
                "data_pulled_this_check": None,
                "time_of_check": pd.Timestamp("2022-12-05 23:49:41.151469+0000", tz="UTC"),
                "metadata_json": {},
            },
        ]
    )

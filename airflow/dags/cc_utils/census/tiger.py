from typing import Dict, List, Union, Optional
from urllib.request import urlretrieve

from bs4 import BeautifulSoup
import geopandas as gpd
import pandas as pd
import requests


def request_page(metadata_url: str) -> requests.models.Response:
    resp = requests.get(metadata_url)
    if resp.status_code == 200:
        return resp
    else:
        raise Exception(f"Couldn't get page metadata for url {metadata_url}")


def scrape_census_ftp_metadata_page(metadata_url: str) -> pd.DataFrame:
    resp = request_page(metadata_url=metadata_url)
    soup = BeautifulSoup(resp.content, "html.parser")
    table = soup.find("table")
    rows = table.find_all("tr")
    table_contents = []
    for row in rows:
        cols = row.find_all("td")
        cols = [col.text.strip() for col in cols]
        table_contents.append(cols)
    table_rows = [el for el in table_contents if len(el) > 0]

    metadata_df = pd.DataFrame(
        [row[1:] for row in table_rows],
        columns=["name", "last_modified", "size", "description"],
    )
    metadata_df["last_modified"] = pd.to_datetime(metadata_df["last_modified"])
    metadata_df["is_dir"] = metadata_df["name"].str.endswith("/")
    metadata_df["clean_name"] = metadata_df["name"].str.replace("/$", "", regex=True)
    metadata_df["is_file"] = (~metadata_df["is_dir"]) & (
        metadata_df["clean_name"] != "Parent Directory"
    )
    while metadata_url.strip().endswith("/"):
        metadata_url = metadata_url[:-1]
    mask = metadata_df["is_file"] | metadata_df["is_dir"]
    metadata_df = metadata_df.loc[mask].copy()
    metadata_df["metadata_url"] = metadata_url + "/" + metadata_df["clean_name"]
    return metadata_df


def get_tiger_vintages_metadata() -> pd.DataFrame:
    all_tiger_vintages_df = scrape_census_ftp_metadata_page(
        metadata_url="https://www2.census.gov/geo/tiger/"
    )
    tiger_vintages_df = all_tiger_vintages_df.loc[
        all_tiger_vintages_df["name"].str.contains("^TIGER\d{4}/", regex=True)
    ].copy()
    tiger_vintages_df = tiger_vintages_df.sort_values(by="name", ignore_index=True)
    return tiger_vintages_df


class TIGERCatalog:
    def __init__(self):
        self.dataset_vintages = get_tiger_vintages_metadata()

    def get_vintage_metadata(self, year: str) -> pd.DataFrame:
        return self.dataset_vintages.loc[self.dataset_vintages["name"] == f"TIGER{year}/"].copy()


class TIGERVintageCatalog:
    def __init__(self, year: str, catalog: Optional[TIGERCatalog] = None):
        self.year = str(year)
        self.set_catalog(catalog=catalog)

    def set_catalog(self, catalog: Optional[TIGERCatalog]) -> None:
        if catalog is None:
            self.catalog = TIGERCatalog()
        else:
            self.catalog = catalog

    @property
    def vintage_metadata(self):
        return self.catalog.get_vintage_metadata(year=self.year)

    @property
    def vintage_entities(self):
        if len(self.vintage_metadata) == 1:
            tiger_vintage_url = self.vintage_metadata["metadata_url"].values[0]
            return scrape_census_ftp_metadata_page(metadata_url=tiger_vintage_url)
        else:
            raise Exception(f"Failed to get unambiguous metadata (got {self.vintage_metadata})")

    def get_entity_metadata(self, entity_name: str) -> pd.DataFrame:
        return self.vintage_entities.loc[self.vintage_entities["clean_name"] == entity_name].copy()

    def print_entity_names(self):
        entity_names = self.vintage_entities.loc[
            self.vintage_entities["is_dir"], "clean_name"
        ].values
        print(f"TIGER Entity options for the {self.year} TIGER vintage:")
        for entity_name in entity_names:
            print(f"  - {entity_name}")
        print(f"Entity count: {len(entity_names)}")


class TIGERGeographicEntityVintage:
    def __init__(self, entity_name: str, year: str, catalog: Optional[TIGERCatalog] = None):
        self.entity_name = entity_name
        self.year = str(year)
        self.vintage_catalog = TIGERVintageCatalog(year=year, catalog=catalog)
        self.entity_metadata = self.vintage_catalog.get_entity_metadata(
            entity_name=self.entity_name
        )

    @property
    def entity_url(self):
        return self.entity_metadata["metadata_url"].values[0]

    @property
    def entity_files_metadata(self):
        return scrape_census_ftp_metadata_page(metadata_url=self.entity_url)

    def get_entity_file_metadata(self, filter_str: str) -> pd.DataFrame:
        return self.entity_files_metadata.loc[
            self.entity_files_metadata["name"].str.contains(filter_str)
        ].copy()

    def get_entity_data(self, filter_str: str) -> gpd.GeoDataFrame:
        entity_subset_metadata = self.get_entity_file_metadata(filter_str=filter_str)
        gdfs = []
        urls = entity_subset_metadata["metadata_url"].values
        for url in urls:
            try:
                gdf = gpd.read_file(url)
                gdfs.append(gdf)
            except Exception:
                print(f"Failed to download {url}")
        full_gdf = pd.concat(gdfs)
        return full_gdf

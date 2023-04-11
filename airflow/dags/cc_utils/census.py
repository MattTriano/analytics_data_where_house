import datetime as dt
from random import random
import re
from time import sleep

from bs4 import BeautifulSoup
import pandas as pd
import requests
from sqlalchemy import select, insert, update
from sqlalchemy.engine.base import Engine

from cc_utils.db import (
    execute_result_returning_query,
    get_reflected_db_table,
    execute_result_returning_orm_query,
)


def check_for_updated_census_table_metadata(engine: Engine):
    page_obj = CensusTableMetadata(metadata_url="https://www2.census.gov/")
    update_df = page_obj.check_warehouse_data_freshness(engine=engine)
    _ = page_obj.insert_current_metadata_freshness_check(engine=engine)
    to_check_mask = update_df["is_dir"] & update_df["updated_metadata_available"]

    metadata_urls_to_check = []
    metadata_urls_to_check.extend(list(update_df.loc[to_check_mask, "metadata_url"]))
    while len(metadata_urls_to_check) > 0:
        metadata_url = metadata_urls_to_check.pop()
        print(f"Checking {metadata_url}")
        sleep_time = 0.5 * random() + 0.5
        sleep(sleep_time)
        page_obj = CensusTableMetadata(metadata_url=metadata_url)
        update_df = page_obj.check_warehouse_data_freshness(engine=engine)
        _ = page_obj.insert_current_metadata_freshness_check(engine=engine)
        to_check_mask = update_df["is_dir"] & update_df["updated_metadata_available"]
        metadata_urls_to_check.extend(list(update_df.loc[to_check_mask, "metadata_url"]))


class CensusTableMetadata:
    BASE_URL = "https://www2.census.gov"

    def __init__(self, metadata_url: str = BASE_URL):
        self.metadata_url = re.sub("/$", "", metadata_url)
        self.page_metadata = self.get_page_metadata()
        self.time_of_check = dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        self.data_freshness_check = self.initialize_data_freshness_check()

    def parse_census_metadata_page(self, resp: requests.models.Response) -> pd.DataFrame:
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
        mask = metadata_df["is_file"] | metadata_df["is_dir"]
        metadata_df.loc[mask, "metadata_url"] = (
            self.metadata_url + "/" + metadata_df.loc[mask, "clean_name"]
        )
        return metadata_df

    def get_page_metadata(self):
        resp = requests.get(self.metadata_url)
        if resp.status_code == 200:
            metadata_df = self.parse_census_metadata_page(resp)
        else:
            raise Exception(f"Couldn't get page metadata for url {self.metadata_url}")
        return metadata_df

    @property
    def child_file_urls(self):
        if len(self.page_metadata) and all(self.page_metadata["clean_name"] == "Parent Directory"):
            return []
        else:
            return [
                f"{self.metadata_url}/{fn}"
                for fn in self.page_metadata.loc[self.page_metadata["is_file"], "clean_name"]
            ]

    @property
    def child_dir_urls(self):
        if len(self.page_metadata) and all(self.page_metadata["clean_name"] == "Parent Directory"):
            return []
        else:
            return [
                f"{self.metadata_url}/{fn}"
                for fn in self.page_metadata.loc[self.page_metadata["is_dir"], "clean_name"]
            ]

    @property
    def last_modified_child(self):
        return self.page_metadata["last_modified"].max()

    def initialize_data_freshness_check(self) -> pd.DataFrame:
        col_order = ["metadata_url", "last_modified", "size", "description", "is_dir", "is_file"]
        df = self.page_metadata[col_order].copy()
        df["time_of_check"] = self.time_of_check
        df = df.loc[df["metadata_url"].notnull()].copy()
        df = df.reset_index(drop=True)
        return df

    def get_prior_metadata_checks_from_db(self, engine: Engine) -> pd.DataFrame:
        metadata_urls = f"""{"', '".join([
            el.replace("'", "''") for el in list(self.data_freshness_check["metadata_url"])
        ])}"""
        results_df = execute_result_returning_query(
            query=f"""
                SELECT *
                FROM metadata.census_metadata
                WHERE metadata_url IN ('{metadata_urls}');
            """,
            engine=engine,
        )
        return results_df

    def check_warehouse_data_freshness(self, engine: Engine):
        check_df = self.get_prior_metadata_checks_from_db(engine=engine)
        check_df = check_df.sort_values(
            by=["metadata_url", "last_modified"], ascending=[True, True], ignore_index=True
        )
        check_df = check_df.drop_duplicates(subset="metadata_url", keep="last", ignore_index=True)
        output_cols = list(self.data_freshness_check.columns)
        output_cols.append("updated_metadata_available")
        update_avail_df = pd.merge(
            left=self.data_freshness_check,
            right=check_df[["metadata_url", "last_modified"]],
            how="left",
            on="metadata_url",
            suffixes=("_source", "_cache"),
        )
        update_avail_df["updated_metadata_available"] = update_avail_df[
            "last_modified_cache"
        ].isnull() | (
            update_avail_df["last_modified_source"] > update_avail_df["last_modified_cache"]
        )
        update_avail_df = update_avail_df.rename(columns={"last_modified_source": "last_modified"})
        update_avail_df = update_avail_df[output_cols].copy()
        return update_avail_df.copy()

    def insert_current_metadata_freshness_check(self, engine):
        update_df = self.check_warehouse_data_freshness(engine=engine)
        if any(update_df["updated_metadata_available"]):
            metadata_table = get_reflected_db_table(
                engine=engine, table_name="census_metadata", schema_name="metadata"
            )
            insert_statement = (
                insert(metadata_table)
                .values(update_df.to_dict(orient="records"))
                .returning(metadata_table)
            )
            result_df = execute_result_returning_orm_query(
                engine=engine, select_query=insert_statement
            )
            return result_df

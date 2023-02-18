import logging

import geopandas as gpd
from shapely.geometry import Point, Polygon, MultiPolygon


def get_empty_geom_of_most_common_col_geom_type(gdf: gpd.GeoDataFrame):
    geom_types = gdf.geometry.geom_type.value_counts(dropna=False)
    if any([geom for geom in geom_types if geom is not None]):
        most_common_geom_type = geom_types.index[0]
        if most_common_geom_type.lower() == "point":
            return Point()
        elif most_common_geom_type.lower() == "polygon":
            return Polygon()
        elif most_common_geom_type.lower() == "multipolygon":
            return MultiPolygon()
        else:
            raise Exception(
                f"most_common_geom_type for given gdf is {most_common_geom_type}, "
                + " which isn't handled yet"
            )
    raise Exception(f"No non-null geometries in given gdf")


def impute_empty_geometries_into_missing_geometries(
    gdf: gpd.GeoDataFrame, logger: logging.Logger
) -> gpd.GeoDataFrame:
    try:
        if any(gdf.geometry.isna()):
            gdf.geometry = gdf.geometry.fillna(
                value=get_empty_geom_of_most_common_col_geom_type(gdf=gdf)
            )
        return gdf
    except TypeError as te:
        logger.error(
            f"Entered object wasn't a GeoDataFrame: {te}, type: {type(te)}",
            exc_info=True,
        )
        raise
    except Exception as e:
        logger.error(
            f"Error while attempting to handle null geom vals: {e}, type: {type(e)}",
            exc_info=True,
        )
        raise

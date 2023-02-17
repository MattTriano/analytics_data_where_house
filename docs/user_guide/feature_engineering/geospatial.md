# Engineering Geospatial Features

PostGIS is a PostgreSQL database extension that adds support for geographic objects and provides [extensive functionality](https://postgis.net/docs/reference.html) for representing and analyzing spatial data.

## Engineering Point Locations from Latitude and Longitude values

If you have `Latitude` and `Longitude` values (and know the Spatial Reference ID, aka SRID, of the system that produced those values), you can produce a geometric `Point` representing the **location** for each `(Latitude, Longitude)` pair.

```sql
SELECT
	parcel_location_id,
    pin,
	ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) AS geometry
FROM clean.cook_county_parcel_locations_clean
```

## Engineering Boundaries from Point Locations

There are a number of algorithms for producing polygons from sets of points.

### Convex Hull
The [Convex Hull](https://postgis.net/docs/ST_ConvexHull.html) algorithm produces a polygon by connecting the outermost points in a group. The `ConvexHull` algorithm is computationally inexpensive, but it will typically produce overlapping polygons.

As a mental heuristic, this algorithm produces a polygon by looping a string around all of the points and pulling tight. 

```sql
WITH school_elem_locs AS (
	SELECT
		school_elem_district,
		ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) AS geometry
	FROM clean.cook_county_parcel_locations_clean
)

SELECT
	school_elem_district,
	ST_ConvexHull(ST_Collect(geometry)) AS school_elem_district_boundary
FROM school_elem_locs
GROUP BY school_elem_district
```

![ConcaveHull over 1.87 million points](/assets/imgs/algorithms/convex_hull_w_runtime.png)

### Concave Hull

The [Concave Hull algorithm](https://postgis.net/docs/ST_ConcaveHull.html) is much (MUCH) more computationally intensive and typically requires some experimentation to dial in the parameters, but it can produce much more complex polygons. The `param_pctconvex` parameter accepts values from 0 to 1, and the lower the value, the shorter the gap between perimeter points before the edge will erode (and the longer the calculation will take).

```sql
SELECT
	school_elem_district,
	ST_ConcaveHull(
		param_geom => ST_Collect(geometry),
		param_pctconvex => 0.25,
         param_allow_holes => false
	) AS school_elem_district_boundary
FROM school_elem_locs
GROUP BY school_elem_district
```

Warning: This algorithm is exceptionally slow over large sets of points. If you absolutely need to use this algorithm, minimize the number of points in the calculation, and start with a higher `param_pctconvex` value and reduce it if needed. For reference, running this query over ~78k points with `param_pctconvex` set to 0.3 took just over 2.5 minutes to finish, but when set to 0.1, it hadn't finished after 38 minutes. 

![param_pctconvex at 10pct over 78k points](/assets/imgs/algorithms/concave_hull_runtime_at_10pct.png)
![param_pctconvex at 30pct over 78k points](/assets/imgs/algorithms/concave_hull_runtime_at_30pct.png)

### Other Polygon-producing Algorithms

There are a few other noteable algorithms for engineering polygons from points, namely variants of Alpha Shape, but they require the SFCGAL PostgreSQL extension which isn't currently included in the PostGIS database container (although adding it would be fairly easy for [new ADWH instances](https://github.com/MattTriano/postgis_geocoder/blob/main/init_files/initdb-postgis.sh)).

[Alpha Shape](https://postgis.net/docs/ST_AlphaShape.html)
[Reference](https://doc.cgal.org/latest/Alpha_shapes_2/index.html#Chapter_2D_Alpha_Shapes)
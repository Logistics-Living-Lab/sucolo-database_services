from typing import Any, Iterable

import geopandas as gpd
from shapely.geometry import Point

def polygons2hexagons(
    gdf: gpd.GeoDataFrame, resolution: int = 9
) -> dict[Any, Iterable[tuple[str, Point]]]: ...

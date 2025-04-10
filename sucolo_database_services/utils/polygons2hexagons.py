from typing import Any, Iterable

import geopandas as gpd
import h3
from shapely.geometry import Point, Polygon


def polygons2hexagons(
    gdf: gpd.GeoDataFrame,
    resolution: int = 9,
) -> dict[Any, Iterable[tuple[str, Point]]]:
    district_hexagons: dict[Any, Iterable[tuple[str, Point]]] = {}
    for distric_id in gdf.index:
        latlon_polygon = _shapely_to_latlngpoly(gdf.loc[distric_id, "geometry"])
        hex_ids: list[str] = h3.polygon_to_cells(latlon_polygon, res=resolution)
        hex_centers = [Point(h3.cell_to_latlng(hex_id)) for hex_id in hex_ids]
        district_hexagons[distric_id] = zip(hex_ids, hex_centers)

    return district_hexagons


def _shapely_to_latlngpoly(geometry: Polygon) -> h3.LatLngPoly:
    exterior = [(lon, lat) for lon, lat in geometry.exterior.coords]
    holes = [
        (lon, lat) for hole in geometry.interiors for lon, lat in hole.coords
    ]
    return h3.LatLngPoly(exterior, holes)

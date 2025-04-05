import geopandas as gpd
from redis import Redis
from redis.typing import ResponseT

from sucolo_database_services.redis_client.consts import HEX_SUFFIX, POIS_SUFFIX
from sucolo_database_services.utils.polygons2hexagons import polygons2hexagons


class RedisWriteRepository:
    def __init__(self, redis_client: Redis, city: str) -> None:
        self.redis_client = redis_client
        self.city = city

    def upload_pois_by_amenity_key(
        self,
        pois: gpd.GeoDataFrame,
        only_wheelchair_accessible: bool = False,
        wheelchair_positive_values: list[str] = ["yes"],
    ) -> list[int]:
        _check_dataframe(pois)
        wheelchair_suffix = ""
        if only_wheelchair_accessible:
            assert "wheelchair" in pois.columns, 'No column "wheelchair" found.'
            pois = pois[pois["wheelchair"].isin(wheelchair_positive_values)]
            wheelchair_suffix = "_wheelchair"

        pipe = self.redis_client.pipeline()

        # Upload pois for each amenity separately
        for amenity in pois["amenity"].unique():
            pois[pois["amenity"] == amenity].apply(
                lambda row: pipe.geoadd(
                    self.city + "_" + amenity + wheelchair_suffix + POIS_SUFFIX,
                    [row["geometry"].x, row["geometry"].y, row.name],
                ),
                axis=1,
            )

        responses = pipe.execute()
        return responses  # type: ignore[no-any-return]

    def upload_hex_centers(
        self, districts: gpd.GeoDataFrame, resolution: int = 9
    ) -> ResponseT:
        hex_centers = polygons2hexagons(districts, resolution=resolution)
        assert len(hex_centers) > 0, "No hexagons were returned."

        values: list[float | str] = []
        for _, district_hex_centers in hex_centers.items():
            for hex_id, hex_center in district_hex_centers:
                values += [hex_center.x, hex_center.y, hex_id]

        response = self.redis_client.geoadd(self.city + HEX_SUFFIX, values)
        return response


def _check_dataframe(gdf: gpd.GeoDataFrame) -> None:
    if "amenity" not in gdf.columns:
        raise ValueError('Expected "amenity" in geodataframe.')
    if "geometry" not in gdf.columns:
        raise ValueError('Expected "geometry" in geodataframe.')

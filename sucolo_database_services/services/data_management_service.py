from pathlib import Path
from typing import Any

import geopandas as gpd

from sucolo_database_services.elasticsearch_client.index_manager import (
    default_mapping,
)
from sucolo_database_services.services.base_service import (
    BaseService,
    BaseServiceDependencies,
)


class _Upload(BaseService):
    def __init__(
        self,
        base_service_dependencies: BaseServiceDependencies,
    ) -> None:
        super().__init__(base_service_dependencies)

    def upload_city_data(
        self,
        city: str,
        pois_gdf: gpd.GeoDataFrame,
        district_gdf: gpd.GeoDataFrame,
        hex_resolutions: int | list[int] = 9,
        ignore_if_index_exists: bool = True,
        es_index_mapping: dict[str, Any] = default_mapping,
    ) -> None:
        """Upload complete city data including POIs, districts, and hexagons."""
        if isinstance(hex_resolutions, int):
            hex_resolutions = [hex_resolutions]
        try:
            self._logger.info(f'UPLOADING DATA FOR CITY "{city}" ...')
            self._upload_city_data_elasticsearch(
                city=city,
                pois_gdf=pois_gdf,
                district_gdf=district_gdf,
                hex_resolutions=hex_resolutions,
                ignore_if_index_exists=ignore_if_index_exists,
                es_index_mapping=es_index_mapping,
            )
            self._upload_city_data_redis(
                city=city,
                pois_gdf=pois_gdf,
                district_gdf=district_gdf,
                hex_resolutions=hex_resolutions,
            )
            self._logger.info(
                f"Successfully uploaded all data for city {city}."
            )
        except Exception as e:
            self._logger.error(
                f"Error uploading city data for {city}: " f"{str(e)}"
            )
            raise

    def _upload_city_data_elasticsearch(
        self,
        city: str,
        pois_gdf: gpd.GeoDataFrame,
        district_gdf: gpd.GeoDataFrame,
        hex_resolutions: list[int],
        ignore_if_index_exists: bool,
        es_index_mapping: dict[str, Any],
    ) -> None:
        """Upload city data to Elasticsearch
        (index, POIs, districts, hexagons)."""
        self._logger.info(f'Creating index "{city}" in elasticsearch.')
        self._es_service.index_manager.create_index(
            index_name=city,
            ignore_if_exists=ignore_if_index_exists,
            mapping=es_index_mapping,
        )
        self._logger.info(f'Index "{city}" created in elasticsearch.')

        self._logger.info("Uploading POIs to elasticsearch.")
        self._es_service.write.upload_pois(index_name=city, gdf=pois_gdf)
        self._logger.info("PoIs uploaded to elasticsearch.")
        self._logger.info("Uploading districts to elasticsearch.")
        self._es_service.write.upload_districts(
            index_name=city, gdf=district_gdf
        )
        self._logger.info("Districts uploaded to elasticsearch.")
        for hex_resolution in hex_resolutions:
            self._logger.info(
                "Uploading hexagons to elasticsearch "
                f"with resolution {hex_resolution}."
            )
            self._es_service.write.upload_hex_centers(
                index_name=city,
                districts=district_gdf,
                hex_resolution=hex_resolution,
            )
        self._logger.info("Hexagons uploaded to elasticsearch.")

    def _upload_city_data_redis(
        self,
        city: str,
        pois_gdf: gpd.GeoDataFrame,
        district_gdf: gpd.GeoDataFrame,
        hex_resolutions: list[int],
    ) -> None:
        """Upload city data to Redis (POIs, wheelchair POIs, hexagons)."""
        self._logger.info(f'Creating keys for city "{city}" in redis.')
        self._redis_service.write.upload_pois_by_amenity_key(
            city=city, pois=pois_gdf
        )
        self._logger.info("PoIs uploaded to redis.")
        self._redis_service.write.upload_pois_by_amenity_key(
            city=city,
            pois=pois_gdf,
            only_wheelchair_accessible=True,
            wheelchair_positive_values=["yes"],
        )
        self._logger.info("Wheelchair accessible PoIs uploaded to redis.")
        for hex_resolution in hex_resolutions:
            self._logger.info(
                "Uploading hexagons to redis "
                f"with resolution {hex_resolution}."
            )
            self._redis_service.write.upload_hex_centers(
                city=city, districts=district_gdf, resolution=hex_resolution
            )
        self._logger.info("Hexagons uploaded to redis.")

    def upload_city_data_from_files(
        self,
        city: str,
        hex_resolutions: int | list[int],
        data_dir: Path = Path("data"),
    ) -> None:
        """Upload city data to the database from files.
        Assumes that Points of Interest (POIs) and districts are stored
        in GeoJSON files in the specified directory structure:
        <data_dir>/
            <city>/
                pois.geojson
                districts.geojson
        """
        pois_gdf, district_gdf = self._load_city_data(
            city=city, data_dir=data_dir
        )
        self.upload_city_data(
            city=city,
            pois_gdf=pois_gdf,
            district_gdf=district_gdf,
            hex_resolutions=hex_resolutions,
            ignore_if_index_exists=True,
        )

    def _load_city_data(
        self, city: str, data_dir: Path = Path("data")
    ) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
        """Get city data from files.
        Assumes that Points of Interest (POIs) and districts are stored
        in GeoJSON files in the specified directory structure:
        <data_dir>/
            <city>/
                pois.geojson
                districts.geojson
        """
        pois_path = data_dir / f"{city}/pois.geojson"
        districts_path = data_dir / f"{city}/districts.geojson"

        if not data_dir.is_dir():
            raise ValueError(f"Data directory {data_dir} does not exist.")
        if not pois_path.is_file():
            raise ValueError(f"POIs file for city {city} does not exist.")
        if not districts_path.is_file():
            raise ValueError(f"Districts file for city {city} does not exist.")

        pois_gdf = gpd.read_file(pois_path)
        district_gdf = gpd.read_file(districts_path).rename(
            columns={"Name": "district"}
        )
        district_gdf = district_gdf.to_crs("EPSG:4326")
        return pois_gdf, district_gdf


class _Delete(BaseService):
    def __init__(
        self,
        base_service_dependencies: BaseServiceDependencies,
    ) -> None:
        super().__init__(base_service_dependencies)

    def delete_city_data(
        self,
        city: str,
        ignore_if_index_not_exist: bool = True,
    ) -> None:
        """Delete all data for a given city from both Elasticsearch and Redis.

        Args:
            city: City name to delete data for
            ignore_if_index_not_exist: Whether to ignore if the index
                doesn't exist
        """
        try:
            self._es_service.index_manager.delete_index(
                index_name=city,
                ignore_if_index_not_exist=ignore_if_index_not_exist,
            )
            self._logger.info(f'Elasticsearch data for city "{city}" deleted.')

            self._redis_service.keys_manager.delete_city_keys(city)
            self._logger.info(f'Redis data for city "{city}" deleted.')
        except Exception as e:
            self._logger.error(
                f"Error deleting city data for {city}: " f"{str(e)}"
            )
            raise


class DataManagementService(_Upload, _Delete):
    """Service for managing data in the database.

    This service provides methods to upload and delete city data
    (POIs, districts, hexagons) in both Elasticsearch and Redis.
    """

    def __init__(
        self,
        base_service_dependencies: BaseServiceDependencies,
    ) -> None:
        super(DataManagementService, self).__init__(base_service_dependencies)

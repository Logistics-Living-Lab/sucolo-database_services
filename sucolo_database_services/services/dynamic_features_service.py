from sucolo_database_services.services.base_service import (
    BaseService,
    BaseServiceDependencies,
)
from sucolo_database_services.services.fields_and_queries import AmenityQuery

HEX_ID_TYPE = str


class DynamicFeaturesService(BaseService):
    """Service for dynamic features - features that requires calculations.
    This service handles nearest distances, presence of amenities,
    and counting POIs within a given distance.
    """

    def __init__(
        self,
        base_service_dependencies: BaseServiceDependencies,
    ) -> None:
        super().__init__(base_service_dependencies)

    def calculate_nearest_distances(
        self,
        query: AmenityQuery,
    ) -> dict[HEX_ID_TYPE, float | None]:
        """Calculate nearest distances for a given amenity type.

        Args:
            city: City name
            query: AmenityQuery containing amenity type and search parameters

        Returns:
            Dictionary mapping hex_id to nearest distance or None
        """
        nearest_distances = (
            self.redis_service.read.find_nearest_pois_to_hex_centers(
                city=query.city,
                amenity=query.amenity,
                resolution=query.resolution,
                radius=query.radius,
                count=1,
            )
        )
        first_nearest_distances = self._nearest_post_processing(
            nearest_distances=nearest_distances,
            radius=query.radius,
            penalty=query.penalty,
        )
        return first_nearest_distances

    def _nearest_post_processing(
        self,
        nearest_distances: dict[str, list[float]],
        radius: int,
        penalty: int | None,
    ) -> dict[str, float | None]:
        """Post-process nearest distances with optional penalty.

        Args:
            nearest_distances: Dictionary of hex_id to list of distances
            radius: Search radius
            penalty: Optional penalty to add when no POI is found

        Returns:
            Dictionary mapping hex_id to processed distance
        """
        if penalty is None:
            first_nearest_distances = {
                hex_id: (dists[0] if len(dists) > 0 else None)
                for hex_id, dists in nearest_distances.items()
            }
        else:
            first_nearest_distances = {
                hex_id: (dists[0] if len(dists) > 0 else radius + penalty)
                for hex_id, dists in nearest_distances.items()
            }
        return first_nearest_distances

    def count_pois_in_distance(
        self,
        query: AmenityQuery,
    ) -> dict[HEX_ID_TYPE, int]:
        """Count POIs within a given radius.

        Args:
            city: City name
            query: AmenityQuery containing amenity type and search parameters

        Returns:
            Dictionary mapping hex_id to count of POIs
        """
        nearest_pois = self.redis_service.read.find_nearest_pois_to_hex_centers(
            city=query.city,
            amenity=query.amenity,
            resolution=query.resolution,
            radius=query.radius,
            count=None,
        )
        counts = {hex_id: len(pois) for hex_id, pois in nearest_pois.items()}
        return counts

    def determine_presence_in_distance(
        self,
        query: AmenityQuery,
    ) -> dict[HEX_ID_TYPE, int]:
        """Determine if any POIs are present within a given radius.

        Args:
            city: City name
            query: AmenityQuery containing amenity type and search parameters

        Returns:
            Dictionary mapping hex_id to presence indicator
            (1 if present, 0 if not)
        """
        nearest_pois = self.redis_service.read.find_nearest_pois_to_hex_centers(
            city=query.city,
            amenity=query.amenity,
            resolution=query.resolution,
            radius=query.radius,
            count=None,
        )
        presence = {
            hex_id: (1 if len(pois) > 0 else 0)
            for hex_id, pois in nearest_pois.items()
        }
        return presence

from typing import Literal

from pydantic import BaseModel, Field, ValidationError, field_validator

HEX_ID_TYPE = str


class Query(BaseModel):
    """Base class for all queries."""

    city: str = Field(..., description="City name to query data for")
    resolution: int = Field(..., description="Hexagon resolution level")

    @field_validator("city")
    def validate_city(cls, city: str) -> str:
        if not city:
            raise ValidationError("City name cannot be empty")
        return city

    @field_validator("resolution")
    def validate_resolution(cls, resolution: int) -> int:
        if resolution <= 0:
            raise ValidationError("Resolution must be a positive integer")
        return resolution


class AmenityFields(BaseModel):
    """Dynamic features query fields for a specific amenity type."""

    amenity: str
    radius: int = Field(gt=0, description="Radius must be positive")
    penalty: int | None = Field(
        default=None, ge=0, description="Penalty must be non-negative"
    )

    @field_validator("radius")
    def validate_radius(cls, radius: int) -> int:
        if radius <= 0:
            raise ValidationError("Radius must be positive")
        return radius


class AmenityQuery(Query, AmenityFields):
    pass


class DistrictFeatureFields(BaseModel):
    """District static features query fields (for each hexagons).
    These values are comming from districts and are the same for all
    hexagons falling into the same district."""

    features: list[str]


class DistrictFeatureQuery(Query, DistrictFeatureFields):
    pass


class MultipleFeaturesQuery(Query):
    nearests: list[AmenityFields] = []
    counts: list[AmenityFields] = []
    presences: list[AmenityFields] = []
    hexagons: DistrictFeatureFields | None = None

    def __post_model_init__(self) -> None:
        def check(
            l_q: list[AmenityFields] | DistrictFeatureFields | None,
        ) -> bool:
            return l_q is None or (
                not isinstance(l_q, DistrictFeatureFields) and len(l_q) == 0
            )

        if (
            check(self.nearests)
            and check(self.counts)
            and check(self.presences)
            and check(self.hexagons)
        ):
            raise ValueError(
                "At least one of the queries type has to be defined."
            )

    @property
    def nearest_queries(self) -> list[AmenityQuery]:
        """Get a list of AmenityQuery for nearests."""
        return self._fields_to_queries("nearests")

    @property
    def count_queries(self) -> list[AmenityQuery]:
        """Get a list of AmenityQuery for counts."""
        return self._fields_to_queries("counts")

    @property
    def presence_queries(self) -> list[AmenityQuery]:
        """Get a list of AmenityQuery for presences."""
        return self._fields_to_queries("presences")

    def _fields_to_queries(
        self,
        type_: Literal["nearests", "counts", "presences"],
    ) -> list[AmenityQuery]:
        """Convert a list of AmenityFields to a list of AmenityQuery."""
        return [
            AmenityQuery(
                city=self.city,
                resolution=self.resolution,
                amenity=fields.amenity,
                radius=fields.radius,
                penalty=fields.penalty,
            )
            for fields in getattr(self, type_)
        ]

from pydantic import BaseModel, Field

HEX_ID_TYPE = str

class Query(BaseModel):
    city: str = Field(..., description="City name to query data for")
    resolution: int = Field(..., description="Hexagon resolution level")
    def validate_city(cls, city: str) -> str: ...
    def validate_resolution(cls, resolution: int) -> int: ...

class AmenityFields(BaseModel):
    amenity: str
    radius: int
    penalty: int | None
    def validate_radius(cls, radius: int) -> int: ...

class AmenityQuery(Query, AmenityFields): ...

class DistrictFeatureFields(BaseModel):
    features: list[str]

class DistrictFeatureQuery(Query, DistrictFeatureFields): ...

class MultipleFeaturesQuery(Query):
    nearests: list[AmenityFields]
    counts: list[AmenityFields]
    presences: list[AmenityFields]
    hexagons: DistrictFeatureFields | None
    def __post_model_init__(self) -> None: ...
    @property
    def nearest_queries(self) -> list[AmenityQuery]: ...
    @property
    def count_queries(self) -> list[AmenityQuery]: ...
    @property
    def presence_queries(self) -> list[AmenityQuery]: ...

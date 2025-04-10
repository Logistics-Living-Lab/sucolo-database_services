class SucoloError(Exception):
    """Base exception for all Sucolo database service errors."""

    pass


class ConfigurationError(SucoloError):
    """Raised when there is a configuration error."""

    pass


class DatabaseError(SucoloError):
    """Base exception for database-related errors."""

    pass


class ElasticsearchError(DatabaseError):
    """Raised when there is an Elasticsearch-specific error."""

    pass


class RedisError(DatabaseError):
    """Raised when there is a Redis-specific error."""

    pass


class CityNotFoundError(SucoloError):
    """Raised when a requested city is not found in the database."""

    pass


class AmenityNotFoundError(SucoloError):
    """Raised when a requested amenity is not found in the database."""

    pass

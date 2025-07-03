from enum import Enum
from pathlib import Path

from pydantic import BaseModel, Field

class Environment(str, Enum):
    DEVELOPMENT = "development"
    TESTING = "testing"
    PRODUCTION = "production"

class DatabaseConfig(BaseModel):
    elastic_host: str
    elastic_user: str
    elastic_password: str
    elastic_timeout: int
    redis_host: str
    redis_port: int
    redis_db: int
    ca_certs: Path = Field(
        default=Path("certs/ca.crt"), description="Path to CA certificates file"
    )
    def validate_ca_certs(cls, v: Path) -> Path: ...

class LoggingConfig(BaseModel):
    level: str = Field(
        default="INFO",
        description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )
    format: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        description="Logging format string",
    )
    file: Path | None = Field(
        default=None, description="Optional log file path"
    )


class Config(BaseModel):
    environment: Environment = Field(
        default=Environment.DEVELOPMENT, description="Current environment"
    )
    database: DatabaseConfig
    logging: LoggingConfig = Field(
        default_factory=LoggingConfig, description="Logging configuration"
    )

    class Config:
        env_prefix: str
        env_file: str
        env_file_encoding: str

from enum import Enum
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class Environment(str, Enum):
    DEVELOPMENT = "development"
    TESTING = "testing"
    PRODUCTION = "production"


class DatabaseConfig(BaseModel):
    elastic_host: str = Field(..., description="Elasticsearch host URL")
    elastic_user: str = Field(..., description="Elasticsearch username")
    elastic_password: str = Field(..., description="Elasticsearch password")
    elastic_timeout: int = Field(
        default=60, description="Elasticsearch timeout in seconds"
    )
    redis_host: str = Field(..., description="Redis host")
    redis_port: int = Field(..., description="Redis port")
    redis_db: int = Field(..., description="Redis database number")
    ca_certs: Path = Field(
        default=Path("certs/ca.crt"), description="Path to CA certificates file"
    )

    @field_validator("ca_certs")
    def validate_ca_certs(cls, v: Path) -> Path:
        if not v.is_file():
            raise ValueError(f"CA certificates file not found at {v}")
        return v


class LoggingConfig(BaseModel):
    level: str = Field(
        default="INFO",
        description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )
    format: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        description="Logging format string",
    )
    file: Optional[Path] = Field(
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
        env_prefix = "SUCOLO_"
        env_file = ".env"
        env_file_encoding = "utf-8"

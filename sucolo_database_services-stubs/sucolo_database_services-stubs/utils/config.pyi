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
    ca_certs: Path
    def validate_ca_certs(cls, v: Path) -> Path: ...

class LoggingConfig(BaseModel):
    level: str = Field(default="INFO")
    format: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    file: Path | None = Field(default=None)

class Config(BaseModel):
    environment: Environment
    database: DatabaseConfig
    logging: LoggingConfig

    class Config:
        env_prefix: str
        env_file: str
        env_file_encoding: str

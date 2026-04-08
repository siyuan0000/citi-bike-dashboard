import os
import logging
from dataclasses import dataclass
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent


@dataclass(frozen=True)
class Settings:
    app_env: str
    db_backend: str
    sqlite_db_path: str
    aws_db_host: str
    aws_db_port: int
    aws_db_name: str
    aws_db_user: str
    aws_db_password: str
    aws_db_sslmode: str
    fetch_interval_seconds: int



def _normalize_env(value: str) -> str:
    return (value or "").strip().lower()



def _load_settings() -> Settings:
    app_env = _normalize_env(os.getenv("APP_ENV", "local"))
    db_backend = _normalize_env(os.getenv("DB_BACKEND", ""))

    # Default backend behavior is controlled by APP_ENV.
    if not db_backend:
        db_backend = "postgres" if app_env == "cloud" else "sqlite"

    sqlite_db_path = os.getenv("SQLITE_DB_PATH", str(BASE_DIR / "citibike.db"))

    return Settings(
        app_env=app_env,
        db_backend=db_backend,
        sqlite_db_path=sqlite_db_path,
        aws_db_host=os.getenv("AWS_DB_HOST", ""),
        aws_db_port=int(os.getenv("AWS_DB_PORT", "5432")),
        aws_db_name=os.getenv("AWS_DB_NAME", ""),
        aws_db_user=os.getenv("AWS_DB_USER", ""),
        aws_db_password=os.getenv("AWS_DB_PASSWORD", ""),
        aws_db_sslmode=os.getenv("AWS_DB_SSLMODE", "require"),
        fetch_interval_seconds=int(os.getenv("FETCH_INTERVAL_SECONDS", "30")),
    )


settings = _load_settings()


def as_dict(mask_secrets=True):
    config_map = {
        "app_env": settings.app_env,
        "db_backend": settings.db_backend,
        "sqlite_db_path": settings.sqlite_db_path,
        "aws_db_host": settings.aws_db_host,
        "aws_db_port": settings.aws_db_port,
        "aws_db_name": settings.aws_db_name,
        "aws_db_user": settings.aws_db_user,
        "aws_db_password": settings.aws_db_password,
        "aws_db_sslmode": settings.aws_db_sslmode,
        "fetch_interval_seconds": settings.fetch_interval_seconds,
    }

    if mask_secrets and config_map["aws_db_password"]:
        config_map["aws_db_password"] = "***"

    return config_map


def print_runtime_config(mask_secrets=True):
    config_map = as_dict(mask_secrets=mask_secrets)
    logging.info("Active runtime config:")
    for key in sorted(config_map.keys()):
        logging.info("  %s=%s", key, config_map[key])
    
    logging.info("-" * 40)



def using_cloud_database() -> bool:
    return settings.db_backend == "postgres"

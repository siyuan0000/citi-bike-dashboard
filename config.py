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
    mysql_db_host: str
    mysql_db_port: int
    mysql_db_name: str
    mysql_db_user: str
    mysql_db_password: str
    fetch_interval_seconds: int



def _normalize_env(value: str) -> str:
    return (value or "").strip().lower()



def _load_settings() -> Settings:
    app_env = _normalize_env(os.getenv("APP_ENV", "local"))
    db_backend = _normalize_env(os.getenv("DB_BACKEND", ""))

    # Default backend behavior is controlled by APP_ENV.
    if not db_backend:
        db_backend = "mysql" if app_env == "cloud" else "sqlite"

    sqlite_db_path = os.getenv("SQLITE_DB_PATH", str(BASE_DIR / "citibike.db"))

    return Settings(
        app_env=app_env,
        db_backend=db_backend,
        sqlite_db_path=sqlite_db_path,
        mysql_db_host=os.getenv("MYSQL_DB_HOST", ""),
        mysql_db_port=int(os.getenv("MYSQL_DB_PORT", "3306")),
        mysql_db_name=os.getenv("MYSQL_DB_NAME", ""),
        mysql_db_user=os.getenv("MYSQL_DB_USER", ""),
        mysql_db_password=os.getenv("MYSQL_DB_PASSWORD", ""),
        fetch_interval_seconds=int(os.getenv("FETCH_INTERVAL_SECONDS", "30")),
    )


settings = _load_settings()


def as_dict(mask_secrets=True):
    config_map = {
        "app_env": settings.app_env,
        "db_backend": settings.db_backend,
        "sqlite_db_path": settings.sqlite_db_path,
        "mysql_db_host": settings.mysql_db_host,
        "mysql_db_port": settings.mysql_db_port,
        "mysql_db_name": settings.mysql_db_name,
        "mysql_db_user": settings.mysql_db_user,
        "mysql_db_password": settings.mysql_db_password,
        "fetch_interval_seconds": settings.fetch_interval_seconds,
    }

    if mask_secrets and config_map["mysql_db_password"]:
        config_map["mysql_db_password"] = "***"

    return config_map


def print_runtime_config(mask_secrets=True):
    config_map = as_dict(mask_secrets=mask_secrets)
    logging.info("Active runtime config:")
    for key in sorted(config_map.keys()):
        logging.info("  %s=%s", key, config_map[key])
    
    logging.info("-" * 40)



def using_cloud_database() -> bool:
    return settings.db_backend == "mysql"

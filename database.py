from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker

from config import settings
from models import Base


def _validate_cloud_config():
    missing = []
    if not settings.mysql_db_host:
        missing.append("MYSQL_DB_HOST")
    if not settings.mysql_db_name:
        missing.append("MYSQL_DB_NAME")
    if not settings.mysql_db_user:
        missing.append("MYSQL_DB_USER")
    if not settings.mysql_db_password:
        missing.append("MYSQL_DB_PASSWORD")

    if missing:
        raise ValueError(
            "Cloud database mode requires these environment variables: " + ", ".join(missing)
        )


def _build_database_url():
    if settings.db_backend == "mysql":
        _validate_cloud_config()
        return URL.create(
            drivername="mysql+pymysql",
            username=settings.mysql_db_user,
            password=settings.mysql_db_password,
            host=settings.mysql_db_host,
            port=settings.mysql_db_port,
            database=settings.mysql_db_name,
        )
    return f"sqlite:///{settings.sqlite_db_path}"


engine = create_engine(
    _build_database_url(),
    pool_pre_ping=True,
    connect_args={"check_same_thread": False} if settings.db_backend == "sqlite" else {},
)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False, expire_on_commit=False)


def get_session():
    return SessionLocal()


def _migrate_status_columns():
    inspector = inspect(engine)
    if "status" not in inspector.get_table_names():
        return

    existing_columns = {col["name"] for col in inspector.get_columns("status")}
    required_columns = {
        "is_installed": "INTEGER",
        "is_renting": "INTEGER",
        "is_returning": "INTEGER",
        "num_bikes_disabled": "INTEGER",
        "num_docks_disabled": "INTEGER",
    }

    with engine.begin() as conn:
        for col_name, col_type in required_columns.items():
            if col_name not in existing_columns:
                conn.execute(text(f"ALTER TABLE status ADD COLUMN {col_name} {col_type}"))


def init_db():
    Base.metadata.create_all(bind=engine)
    _migrate_status_columns()
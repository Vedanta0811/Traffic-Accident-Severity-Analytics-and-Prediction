"""
Database connection manager supporting PostgreSQL with transparent fallback to SQLite.
"""

import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("db_manager")

POSTGRES_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/traffic_accidents")
SQLITE_PATH = os.getenv("SQLITE_FALLBACK_PATH", "data/traffic_accidents.db")


def get_engine():
    """
    Returns an active SQLAlchemy engine. Tries PostgreSQL first;
    if connection fails, falls back to SQLite.
    """
    # 1. Try PostgreSQL if configured
    if POSTGRES_URL:
        try:
            pg_engine = create_engine(POSTGRES_URL, pool_pre_ping=True, connect_args={"connect_timeout": 3})
            with pg_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Successfully connected to PostgreSQL warehouse.")
            return pg_engine
        except (OperationalError, Exception) as e:
            logger.warning(f"PostgreSQL connection failed ({e}). Falling back to SQLite.")

    # 2. Fallback to SQLite
    sqlite_file = Path(SQLITE_PATH)
    sqlite_file.parent.mkdir(parents=True, exist_ok=True)
    sqlite_url = f"sqlite:///{sqlite_file.resolve()}"
    logger.info(f"Using local SQLite database at: {sqlite_url}")
    return create_engine(sqlite_url, connect_args={"check_same_thread": False})


def init_db(engine=None):
    """
    Executes DDL schemas to ensure tables and views exist.
    """
    if engine is None:
        engine = get_engine()

    sql_dir = Path(__file__).resolve().parent.parent.parent / "sql"
    ddl_files = ["01_staging_and_quarantine.sql", "02_star_schema.sql"]

    is_sqlite = "sqlite" in str(engine.url)

    with engine.begin() as conn:
        for file_name in ddl_files:
            file_path = sql_dir / file_name
            if file_path.exists():
                logger.info(f"Applying schema: {file_name}")
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read()

                # Adjust PostgreSQL specific syntax for SQLite if running on SQLite
                if is_sqlite:
                    content = content.replace("SERIAL PRIMARY KEY", "INTEGER PRIMARY KEY AUTOINCREMENT")
                    content = content.replace("TIMESTAMP WITH TIME ZONE", "TIMESTAMP")
                    content = content.replace("NUMERIC(10, 6)", "REAL")
                    content = content.replace("NUMERIC(5, 2)", "REAL")
                    content = content.replace("NUMERIC(8, 2)", "REAL")
                    content = content.replace("BOOLEAN", "INTEGER")

                # Split statements by semicolon
                statements = [s.strip() for s in content.split(";") if s.strip()]
                for stmt in statements:
                    try:
                        conn.execute(text(stmt))
                    except Exception as ex:
                        logger.debug(f"Statement notice: {ex}")

        # Ensure Dim Severity is seeded
        seed_severity = [
            {"severity_key": 1, "severity_code": 1, "severity_name": "Fatal", "severity_description": "Accidents involving at least one fatality"},
            {"severity_key": 2, "severity_code": 2, "severity_name": "Serious", "severity_description": "Accidents involving severe/hospitalizing injuries"},
            {"severity_key": 3, "severity_code": 3, "severity_name": "Slight", "severity_description": "Accidents involving minor or slight injuries"},
        ]
        for item in seed_severity:
            try:
                conn.execute(
                    text("""
                        INSERT OR IGNORE INTO dim_severity (severity_key, severity_code, severity_name, severity_description)
                        VALUES (:severity_key, :severity_code, :severity_name, :severity_description)
                    """ if is_sqlite else """
                        INSERT INTO dim_severity (severity_key, severity_code, severity_name, severity_description)
                        VALUES (:severity_key, :severity_code, :severity_name, :severity_description)
                        ON CONFLICT (severity_key) DO NOTHING;
                    """),
                    item
                )
            except Exception:
                pass

    logger.info("Database schemas successfully initialized.")


if __name__ == "__main__":
    eng = get_engine()
    init_db(eng)

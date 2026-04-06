from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.settings import get_settings


settings = get_settings()

engine = None
SessionLocal = None

if settings.data_mode == "postgis":
    engine = create_engine(
        settings.database_url,
        pool_pre_ping=True,
        future=True,
    )

    SessionLocal = sessionmaker(
        bind=engine,
        autocommit=False,
        autoflush=False,
        future=True,
    )

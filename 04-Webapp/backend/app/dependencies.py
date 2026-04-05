from collections.abc import Generator

from sqlalchemy.orm import Session

from app.core.settings import Settings, get_settings
from app.database import SessionLocal


def get_db() -> Generator[Session | None, None, None]:
    if SessionLocal is None:
        yield None
        return

    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_app_settings() -> Settings:
    return get_settings()

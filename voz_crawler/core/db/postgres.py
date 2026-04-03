from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


class PostgresClient:
    """SQLAlchemy engine wrapper with production-ready pool settings."""

    def __init__(self, url: str) -> None:
        self._engine = create_engine(
            url,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
        )

    @property
    def engine(self) -> Engine:
        return self._engine

    def dispose(self) -> None:
        self._engine.dispose()

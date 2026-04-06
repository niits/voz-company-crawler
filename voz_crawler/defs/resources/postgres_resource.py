from dagster import ConfigurableResource, InitResourceContext
from pydantic import PrivateAttr
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from voz_crawler.core.repository.raw_repository import RawRepository


class PostgresResource(ConfigurableResource):
    """Connection parameters for the target PostgreSQL database.

    Fields are resolved from environment variables at runtime via EnvVar.
    The SQLAlchemy engine is created once per process in setup_for_execution
    and disposed in teardown_after_execution.
    """

    user: str
    password: str
    host: str
    port: int
    db: str
    raw_schema: str
    posts_table: str

    _engine: Engine = PrivateAttr()

    def setup_for_execution(self, context: InitResourceContext) -> None:
        self._engine = create_engine(self.url)

    def teardown_after_execution(self, context: InitResourceContext) -> None:
        self._engine.dispose()

    @property
    def url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"

    def get_repository(self) -> RawRepository:
        return RawRepository(engine=self._engine, schema=self.raw_schema, table=self.posts_table)

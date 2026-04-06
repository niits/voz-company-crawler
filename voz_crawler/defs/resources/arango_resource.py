from arango import ArangoClient
from dagster import ConfigurableResource, InitResourceContext
from pydantic import PrivateAttr

from voz_crawler.core.repository.graph_repository import GraphRepository


class ArangoDBResource(ConfigurableResource):
    """Connection parameters for ArangoDB.

    Fields are resolved from environment variables at runtime via EnvVar.
    The authenticated database handle is created once per process in
    setup_for_execution, including creating the target database if absent.
    """

    host: str
    port: int
    db: str
    username: str
    password: str

    _db: object = PrivateAttr()

    def setup_for_execution(self, context: InitResourceContext) -> None:
        client = ArangoClient(hosts=f"http://{self.host}:{self.port}")
        sys_db = client.db("_system", username=self.username, password=self.password)
        if not sys_db.has_database(self.db):
            sys_db.create_database(self.db)
        self._db = client.db(self.db, username=self.username, password=self.password)
        GraphRepository(db=self._db).ensure_schema()

    def get_repository(self) -> GraphRepository:
        return GraphRepository(db=self._db)

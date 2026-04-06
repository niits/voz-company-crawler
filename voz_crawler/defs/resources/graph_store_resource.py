from dagster import ConfigurableResource, InitResourceContext
from pydantic import PrivateAttr

from voz_crawler.core.repository.graph_repository import GraphRepository


class ArangoGraphStoreResource(ConfigurableResource):
    host: str
    port: int = 8529
    db: str
    username: str
    password: str

    _repo: object = PrivateAttr()

    def setup_for_execution(self, context: InitResourceContext) -> None:
        from arango import ArangoClient

        from voz_crawler.core.repository.arango_repository import ArangoGraphRepository

        client = ArangoClient(hosts=f"http://{self.host}:{self.port}")
        sys_db = client.db("_system", username=self.username, password=self.password)
        if not sys_db.has_database(self.db):
            sys_db.create_database(self.db)
        db = client.db(self.db, username=self.username, password=self.password)
        self._repo = ArangoGraphRepository(db=db)
        self._repo.ensure_schema()

    def get_repository(self) -> GraphRepository:
        return self._repo  # type: ignore[return-value]


class Neo4jGraphStoreResource(ConfigurableResource):
    uri: str
    username: str
    password: str
    database: str = "neo4j"

    _repo: object = PrivateAttr()
    _driver: object = PrivateAttr(default=None)

    def setup_for_execution(self, context: InitResourceContext) -> None:
        from neo4j import GraphDatabase

        from voz_crawler.core.repository.neo4j_repository import Neo4jGraphRepository

        driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))
        self._driver = driver

        # Community edition only supports the default "neo4j" database.
        # Attempt to create the target db; swallow errors on community.
        try:
            with driver.session(database="system") as s:
                s.run("CREATE DATABASE $db IF NOT EXISTS", db=self.database)
        except Exception:
            pass

        self._repo = Neo4jGraphRepository(driver=driver, database=self.database)
        self._repo.ensure_schema()

    def teardown_after_execution(self, context: InitResourceContext) -> None:
        if self._driver is not None:
            self._driver.close()

    def get_repository(self) -> GraphRepository:
        return self._repo  # type: ignore[return-value]

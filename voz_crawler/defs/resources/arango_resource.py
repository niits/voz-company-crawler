from arango import ArangoClient
from dagster import ConfigurableResource


class ArangoDBResource(ConfigurableResource):
    """Connection parameters for ArangoDB.

    Fields are resolved from environment variables at runtime via EnvVar.
    `get_db()` creates the target database if it doesn't exist, then returns
    an authenticated Database handle ready for collection/graph operations.
    """

    host: str
    port: int = 8529
    db: str
    username: str = "root"
    password: str

    def get_db(self):
        """Return an authenticated ArangoDB Database handle.

        Creates the target database on first call if it doesn't exist yet.
        Each call opens a new HTTP session — callers should not cache the result
        across asset boundaries.
        """
        client = ArangoClient(hosts=f"http://{self.host}:{self.port}")
        sys_db = client.db("_system", username=self.username, password=self.password)
        if not sys_db.has_database(self.db):
            sys_db.create_database(self.db)
        return client.db(self.db, username=self.username, password=self.password)

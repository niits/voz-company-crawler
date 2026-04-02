from dagster import ConfigurableResource


class ArangoResource(ConfigurableResource):
    """Connection parameters for ArangoDB.

    `get_client()` returns an authenticated python-arango Database handle.
    """

    host: str
    port: int = 8529
    username: str = "root"
    password: str
    db: str = "voz_graph"

    def get_client(self):
        from arango import ArangoClient

        client = ArangoClient(hosts=f"http://{self.host}:{self.port}")
        return client.db(self.db, username=self.username, password=self.password)

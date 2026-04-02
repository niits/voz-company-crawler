from dagster import ConfigurableResource


class ArangoResource(ConfigurableResource):
    """Connection parameters for ArangoDB.

    `get_client()` returns an authenticated python-arango Database handle.
    """

    host: str
    port: int = 8529
    password: str
    db: str = "voz_graph"

    def get_client(self):
        from arango import ArangoClient

        client = ArangoClient(hosts=f"http://{self.host}:{self.port}")
        return client.db(self.db, username="root", password=self.password)

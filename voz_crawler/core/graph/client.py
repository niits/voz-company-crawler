from arango import ArangoClient

from voz_crawler.core.graph.schema import ensure_schema


class ArangoGraphClient:
    """Authenticated ArangoDB connection with lazy schema initialization.

    Calls ensure_schema() exactly once on first db() access. Subsequent
    calls return the same Database object — no new TCP handshakes.
    """

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        db_name: str,
    ) -> None:
        client = ArangoClient(hosts=f"http://{host}:{port}")
        self._db = client.db(db_name, username=username, password=password)
        self._schema_initialized = False

    def db(self):
        """Return the authenticated Database, initializing schema on first call."""
        if not self._schema_initialized:
            ensure_schema(self._db)
            self._schema_initialized = True
        return self._db

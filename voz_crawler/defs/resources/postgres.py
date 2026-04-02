from dagster import ConfigurableResource


class PostgresResource(ConfigurableResource):
    """Connection parameters for the target PostgreSQL database."""

    user: str
    password: str
    host: str
    port: int = 5432
    db: str

    @property
    def url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"

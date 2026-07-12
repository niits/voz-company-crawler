from dagster import ConfigurableResource, InitResourceContext
from langfuse import Langfuse, get_client
from pydantic import PrivateAttr
from pydantic_ai import Agent


class LangfuseResource(ConfigurableResource):
    """Optional Langfuse Cloud tracing.

    get_client() reads LANGFUSE_PUBLIC_KEY / LANGFUSE_SECRET_KEY / LANGFUSE_BASE_URL
    from the environment. When they're unset, the SDK returns a disabled client and
    every traced call downstream becomes a no-op — the pipeline behaves identically
    either way, so there are no config fields to declare here.
    """

    _client: Langfuse = PrivateAttr()

    def setup_for_execution(self, context: InitResourceContext) -> None:
        self._client = get_client()
        Agent.instrument_all()

    def get_client(self) -> Langfuse:
        return self._client

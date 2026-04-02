from voz_crawler.defs.resources.arango import ArangoResource


def test_arango_resource_fields():
    resource = ArangoResource(
        host="localhost",
        port=8529,
        password="secret",
        db="test_db",
    )
    assert resource.host == "localhost"
    assert resource.port == 8529
    assert resource.db == "test_db"


def test_arango_resource_default_port():
    resource = ArangoResource(host="arango", password="pw", db="mydb")
    assert resource.port == 8529

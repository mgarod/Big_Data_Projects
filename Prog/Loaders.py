import CassandraLoader as cload
import Neo4jLoader as nload


def load_all():
    cload.load_cassandra()
    nload.load_neo4j()

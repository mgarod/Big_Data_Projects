import CassandraLoader as cload
import Neo4jLoader as nload
from py2neo import Graph

def load_all():
    cload.load_cassandra()
    nload.load_neo4j()
    

load_all()

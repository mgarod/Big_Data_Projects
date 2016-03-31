from py2neo import Graph
import os


graph = Graph()  # Makes connection to http://127.0.0.1:7474

cypher = graph.cypher
currentDir = os.path.realpath(__file__)  # Get Path to Neo4jLoader.py
currentDir = currentDir[:-15]  # Get Path to folder containing Neo4jLoader.py

def load_neo4j():
    graph.delete_all()
    create_distanceGraph()
    create_PersonGraph()
    create_ProjectGraph()
    create_Relationship()
    create_InterestGraph()


def create_distanceGraph():
    #refer to loadDisGraph.cpl for comments on commands in execute

    #execute indexing command
    cypher.execute("""
        CREATE CONSTRAINT ON (o:Organization)
        ASSERT o.name IS UNIQUE;
    """)

    # creating Organization node
    statement1 = """
        LOAD CSV FROM 'file://%s/Data/orgs.csv' AS line with line skip 1
        Merge (o:Organization {name: TOUPPER(line[1])})
    """
    statement1 %= currentDir
    cypher.execute(statement1)



    #create relationships between oraganization
    statement2 = """
        LOAD CSV  FROM 'file://%s/Data/distance.csv' AS line
        MATCH (company1:Organization { name: TOUPPER(line[0])})
        MATCH (company2:Organization { name: TOUPPER(line[1])})
        MERGE (company1)-[m:DISTANCE]-(company2)
        SET m.miles = TOINT(line[2]),
        m.closeby = CASE WHEN TOINT(line[2]) < 10 THEN true ELSE false END
    """
    statement2 %= currentDir
    cypher.execute(statement2)


def create_PersonGraph():
    #refer to loadDisGraph.cpl for comments on commands in execute

    # execute indexing command
    cypher.execute("CREATE CONSTRAINT ON (P:Person) ASSERT P.User_id IS UNIQUE")

    # creating person graph
    statement1 = """
        LOAD CSV FROM 'file://%s/Data/names.csv' AS line with line skip 1
        MERGE (p:Person { User_id: TOINT(line[0]),
        Fname: upper(line[1]), Lname: upper(line[2])} )
    """
    statement1 %= currentDir
    cypher.execute(statement1)




def create_ProjectGraph():
    # creating project graph

    statement1 = """
        LOAD CSV FROM 'file://%s/Data//projects.csv' AS line with line skip 1
        MERGE(i:projects { projectname: Tolower(line[1]) } )
    """
    statement1 %= currentDir
    cypher.execute(statement1)


    # creating relationships between person and projects
    statement2 = """
        LOAD CSV FROM 'file://%s/Data//projects.csv' AS line with line skip 1
        MATCH (a:Person { User_id: TOINT(line[0])}),
        (b:projects { projectname: tolower(line[1]) })
        MERGE (a)-[:Working_on]->(b)
    """

    statement2 %= currentDir
    cypher.execute(statement2)


def create_Relationship():
    # creating relationships between person and organization
    statement1 = """
        LOAD CSV FROM 'file://%s/Data//orgs.csv' AS line with line skip 1
        MATCH (a:Person { User_id: TOINT(line[0]) }),
            (b:Organization { name:TOUPPER(line[1]) })
        CREATE (a)-[:Work_at]->(b)

    """
    statement1 %= currentDir
    cypher.execute(statement1)

    # creating colleague relationships
    statement2 = """
        LOAD CSV  FROM 'file://%s/Data//orgs.csv' AS line with line skip 1
        MATCH (thisPerson{User_id: TOINT(line[0]) })-[:Working_on]->(project)<-[:Working_on]-(Person)
        WHERE NOT (thisPerson)-[:Working_on]-(Person)
        MERGE(thisPerson)-[:colleague]-(Person)
    """
    statement2 %= currentDir
    cypher.execute(statement2)


def create_InterestGraph():
    # creating interest node
    statement1 = """
        LOAD CSV FROM 'file://%s/Data/interests.csv' AS line with line skip 1
        MERGE (s:interest {interestName: tolower(line[1])})
    """
    statement1 %= currentDir
    cypher.execute(statement1)

    # creating relations between interest and person
    statement2 = """
        LOAD CSV FROM 'file://%s/Data/interests.csv' AS line with line skip 1
        MATCH (a:Person {User_id: TOINT(line[0]) }), (b:interest {interestName: tolower(line[1])})
        MERGE(a)-[r:interested_in]->(b)
        SET r.level = TOINT(line[2])
    """
    statement2 %= currentDir
    cypher.execute(statement2)


load_neo4j()

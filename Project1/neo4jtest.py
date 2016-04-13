from py2neo import Graph, Node, Relationship
import os


graph = Graph()  # Makes connection to http://localhost:7474
graph.delete_all()  # Clear the graph

Michael = Node("Person", name="Michael", job="Student", age=26)
Felice = Node("Person", name="Felice", job="Web Developer", age=32)
Jackie = Node("Person", name="Jackie", job="Graphic Designer", age=31)
Anny = Node("Person", name="Anny", job="Consultant", age=32)

Michael["likes"] = ["food", "beer", "wine"]

Michael_dates_Felice = Relationship(Michael, "Knows", Felice, Since=2015)
Felice_knows_Jackie = Relationship(Felice, "Knows", Jackie, Since=1990)
Anny_knows_Michael = Relationship(Anny, "Knows", Michael, Since=2016)

graph.create(Felice_knows_Jackie)
graph.create(Michael_dates_Felice)
graph.create(Anny_knows_Michael)

query = """
    MATCH (a:Person)-[k:Knows]->(b:Person)
    WHERE b.name="%s"
    RETURN a.name, b.name
"""
query %= "Michael"
print graph.cypher.execute(query)

# path = os.path.realpath(__file__)
# query = """
#     LOAD CSV WITH HEADERS FROM "file:///%s/Data/names.csv" as line
#     MERGE (p:Person
#         {User_id: TOINT(line.User_id),
#         Fname: upper(line.First_Name),
#         Lname: upper(line.Last_Name)
#         }
#     )
#     """
#
# query %= path
#
# print query

# Visit http://localhost:7474 to view the GUI and practice CypherQL

# Get every person who with any relation to "Felice"
# MATCH (a:Person)-[*]->(b:Person) WHERE b.name="Felice" RETURN a.name, b.name

# Get every person who knows anyone
# MATCH (a:Person)-[k:Knows]->(b:Person) RETURN a.name,k.Since, b.name

# Match any person who knows some person that knows a third person
# MATCH (a:Person)-[:Knows]->(b:Person)-[:Knows]->(c:Person) Return a.name, b.name, c.name
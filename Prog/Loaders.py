import CassandraLoader as cload
from py2neo import Graph

def load_all():
    cload.load_cassandra()

    graph = Graph()  # Makes connection to http://127.0.0.1:7474
    cypher = graph.cypher
    
    #refer to loadDisGraph.cpl for comments on commands in execute
    
    #execute indexing command
    cypher.execute("CREATE CONSTRAINT ON (o:Organization) ASSERT o.name IS UNIQUE;")
    
    # creating Oragnization node
    cypher.execute("LOAD CSV WITH HEADERS FROM 'file:////home//ryan/Desktop/BigData/code/distance.csv' AS line "
                   "MERGE (o:Organization { name: line.`Organization 1`, name: line.`Organization 2` } ) ")
    
    #create relationships between oraganization
    cypher.execute("LOAD CSV WITH HEADERS FROM 'file:////home//ryan/Desktop/BigData/code/distance.csv' AS line "
                   "MATCH (company1:Organization { name: line.`Organization 1`}) "
                   "MATCH (company2:Organization { name: line.`Organization 2`})"
                   "MERGE (company1)-[m:DISTANCE]-(company2)"
                   "SET m.miles = TOINT(line.Distance),"
    	           "m.closeby = CASE WHEN TOINT(line.Distance) < 10 THEN true ELSE false END ")
    
    
    #refer to loadDisGraph.cpl for comments on commands in execute
    
    # execute indexing command
    cypher.execute("CREATE CONSTRAINT ON (P:Person) ASSERT P.User_id IS UNIQUE")
    # creating person graph
    cypher.execute("LOAD CSV WITH HEADERS FROM 'file:////home//ryan/Desktop/BigData/code/names.csv' AS line "
                   "MERGE (p:Person { User_id: TOINT(line.User_id),"
                   " Fname: upper(line.`first name`), Lname: upper(line.`last name`) } ) ")
    
    # creating person graph
    cypher.execute("LOAD CSV WITH HEADERS FROM "
                   "'file:////home//ryan/Desktop/BigData/code/projects.csv' AS line "
                   "MERGE(i:projects { projectname: line.Projects } )")
    
    # creating relationships between person and projects
    cypher.execute("LOAD CSV WITH HEADERS FROM "
                   "'file:////home//ryan/Desktop/BigData/code/projects.csv' AS line "
                   "MATCH (a:Person { User_id: TOINT(line.User_id) }),(b:projects { projectname:line.Projects })"
                   "MERGE (a)-[:Working_on]->(b)")
    
    # creating relationships between person and organization
    cypher.execute("LOAD CSV WITH HEADERS FROM "
                   "'file:////home//ryan/Desktop/BigData/code/orgs.csv' AS line  "
                   "MATCH (a:Person { User_id: TOINT(line.User_id) }),(b:Organization { name:line.organization })"
                   "CREATE (a)-[:Work_at]->(b)")
    
    # creating colleague relationships
    cypher.execute("LOAD CSV WITH HEADERS FROM "
                   "'file:////home//ryan/Desktop/BigData/code/orgs.csv' AS line "
                   "MATCH (thisPerson{ User_id: TOINT(line.User_id) })-[:Working_on]->(project)<-[:Working_on]-(Person)"
                   "WHERE NOT (thisPerson)-[:Working_on]-(Person)"
                   "MERGE(thisPerson)-[:colleague]-(Person)")
    
    # creating interest node
    cypher.execute("LOAD CSV WITH HEADERS FROM 'file:////home//ryan/Desktop/BigData/code/interests.csv' AS line "
                   "MERGE (s:interest {interestName: line.Interest})")
    
    # creating relations between interest and
    cypher.execute("LOAD CSV WITH HEADERS FROM "
                   "'file:////home//ryan/Desktop/BigData/code/interests.csv' AS line "
                   "MATCH (a:Person {User_id: TOINT(line.User_id) }), (b:interest {interestName: line.Interest}) "
                   "MERGE(a)-[:interested_in]->(b)")

load_all()
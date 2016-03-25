from py2neo import Graph


def trusted_colleague(Fname, Lname):
    graph = Graph()  # Makes connection to http://127.0.0.1:7474
    cypher = graph.cypher
    result = cypher.execute(
        "MATCH (p:Person{Fname: upper({first_name}), Lname: upper({last_name})})-[r:colleague*2]->(Similar_p)  "
        "MATCH (Similar_p)-[ri:interested_in]->(s)  "
        "MATCH (p)-[:interested_in]->(common_interest)<-[:interested_in]-(Similar_p)   "
        "return DISTINCT Similar_p.Fname, Similar_p.Lname", first_name = Fname, last_name = Lname)
    print result.records


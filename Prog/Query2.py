from py2neo import Graph
graph = Graph()  # Makes connection to http://127.0.0.1:7474
cypher = graph.cypher

def query2(Fname, Lname):

    result = cypher.execute(
        "MATCH (p:Person{Fname: upper({first_name}), Lname: upper({last_name})})-[r:colleague*2]-(Similar_p)  "
        "WHERE NOT  (p)-[:colleague]-(Similar_p)"
        "MATCH (p)-[:interested_in]->(common_interest)<-[:interested_in]-(Similar_p)   "
        "return DISTINCT Similar_p.Fname AS firstName, Similar_p.Lname AS lastName", first_name = Fname, last_name = Lname)



    #return false if person is not found in database
    if not PersonExist(Fname, Lname):
        print 'This person does not exist in Database.'
        return

    #check if any result were found and return
    if result.__len__() == 0:
        print 'No trusted colleague of colleague were found that share a common interest.'
        return

    print 'Trusted colleague of colleague that share a common interest:'

    for index in result:
        print index.firstName, index.lastName



def PersonExist(Fname, Lname):

    result = cypher.execute(
        "MATCH (p:Person{Fname: upper({first_name}), Lname: upper({last_name}) } ) return p",
        first_name = Fname, last_name = Lname )

    if result.__len__() > 0:
        return True
    else:
        return False

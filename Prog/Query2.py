from py2neo import Graph
import Validators as valid
import Query3

graph = Graph()  # Makes connection to http://127.0.0.1:7474


def query2():
    #get user id
    u_id = valid.validate_num("$ Enter a user_id: ")

    # User Cassandra to verify existence for speed
    if not Query3.user_exists(u_id):
        print "User ID {} was not found".format(u_id)
        return

    #get the resource data from the query
    result = graph.cypher.execute("""
        MATCH (p:Person{User_id: {Uid} })-[r:colleague*2]-(Similar_p)
        WHERE NOT  (p)-[:colleague]-(Similar_p)
        MATCH (p)-[:interested_in]->(common_interest)<-[:interested_in]-(Similar_p)
        return DISTINCT  Similar_p.Fname, Similar_p.Lname, p.Fname, p.Lname
        """, Uid = u_id )

    if result.__len__() == 0:
        print 'No results found for User id:' + u_id
        return

    print 'Trusted colleague of colleague that share a common interest with', result[0][2], result[0][3]

    k = 1
    for index in result:
        print "{} | {} {}".format(k,index[0], index[1])
        k+=1

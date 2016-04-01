from py2neo import Graph
import Validators as valid
import Query3

graph = Graph()  # Makes connection to http://127.0.0.1:7474


def query2():
    #get user id and interest name from user
    u_id = valid.validate_num("$ Enter a user_id: ")
    u_interest = raw_input("$ Enter an Interest: ")
    u_interest.lower()

    # User Cassandra to verify existence for speed
    if not Query3.user_exists(u_id):
        print "User ID {} was not found".format(u_id)
        return

    #get the resource data from the query
    result = graph.cypher.execute("""
        MATCH (p:Person{User_id: {Uid} })-[r:colleague*2]-(Similar_p)
        WHERE NOT  (p)-[:colleague]-(Similar_p)
        MATCH (Similar_p)-[:interested_in]->(interest) where interest.interestName = {uint}
        return DISTINCT  Similar_p.Fname, Similar_p.Lname, p.Fname, p.Lname, interest.interestName
        """, Uid = u_id, uint = u_interest)
    
    #if not data is found in search
    if result.__len__() == 0:
        print 'No results found'
        return

    print 'Trusted colleague of colleague that are interested in {} :'.format( u_interest)

    k = 1
    for index in result:
        print "{} | {} {}".format(k,index[0], index[1])
        k+=1

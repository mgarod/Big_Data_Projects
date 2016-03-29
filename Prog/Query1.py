from py2neo import Graph
import sys
import Validators as valid
import Query3

def query1():
    # Get nameF, nameL within this function
    u_id = valid.validate_num("$ Enter a user_id: ")

    if not Query3.user_exists(u_id):
        print "User ID {} was not found".format(u_id)
        return

    graph = Graph()  # Makes connection to http://127.0.0.1:7474
    cypher = graph.cypher
    mylist = set()
    namelist = set()

    #get the resource data from the query
    result = cypher.execute(
        """
        Match (person:Person {User_id: {uid}})-[r:Work_at]->(job)
        Match (job)-[r1:DISTANCE]-(closeCompany) where r1.closeby = true
        Match (closePerson)-[:Work_at]->(closeCompany)
        Match (closePerson)-[ri:interested_in]->(cpInterest)<-[ri2:interested_in]-(person)
        return closePerson.Fname AS PersonF, closePerson.Lname AS PersonL
        , closePerson.User_id AS Personid, TOINT(ri2.level) AS level,closeCompany.name AS company,ri.level AS OtherLevel ,
        cpInterest.interestName order BY TOINT( ri2.level) DESC
        UNION
        Match (person:Person {User_id: {uid}})-[r:Work_at]->(closeCompany)
        Match (closePerson)-[:Work_at]->(closeCompany)
        Match (closePerson)-[ri:interested_in]->(cpInterest)<-[ri2:interested_in]-(person)
        return closePerson.Fname AS PersonF, closePerson.Lname AS PersonL
        , closePerson.User_id AS Personid, TOINT(ri2.level) AS level,closeCompany.name AS company,ri.level AS OtherLevel,
        cpInterest.interestName order BY TOINT( ri2.level) DESC
        """,uid=u_id)
    sys.stdout.flush()

    #get ranking order
    table = {}
    for index in result:
        if table.has_key(index.Personid):
            table[index.Personid] = table[index.Personid] + min(index.level, index.OtherLevel)
        else:
            table[index.Personid] = min(index.level, index.OtherLevel)

    #put inorder
    sortedarray = table.items()
    sortedarray.sort(None, key = lambda x: x[1], reverse = False)
    print '\n'
    
    #read data out
    while sortedarray.__len__() != 0:
    
        current = sortedarray.pop()
        #print common interest for a certain person
        for x in result:

            if mylist.__contains__(x[5]):
                continue

                #find p in x# namelist = set() # reset the interest set
            if (current[0] == x[2]) :

                if(not namelist.__contains__(current[0])):

                    fullname ="Name: "
                    fullname =fullname + x[0] + " " + x[1] + ", Working at: "+ x[4] +"  User id: "
                    print fullname, current[0]

                print "Common Interest: {} ({}) ".format( x[6], x[3])

                namelist.add(current[0])
                mylist.add(x[5])

        mylist = set()
        namelist = set() # reset the interest set
        print '\n'





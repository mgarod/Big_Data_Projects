from py2neo import Graph



def query1(nameF, nameL ):
    graph = Graph()  # Makes connection to http://127.0.0.1:7474
    cypher = graph.cypher
    list = set()
    namelist = set()


    #get the resource data from the query
    result = cypher.execute(
                    "Match (person:Person {Fname: {name1}, Lname: { name2}})-[r:Work_at]->(job) "
                    "Match (job)-[r1:DISTANCE]-(closeCompany) where r1.closeby = true "
                    "Match (closePerson)-[:Work_at]->(closeCompany) "
                    "Match (closePerson)-[ri:interested_in]->(cpInterest)<-[:interested_in]-(person) "
                    "return closePerson.Fname AS PersonF, closePerson.Lname AS PersonL"
                    ", closePerson.User_id AS Personid, TOINT(cpInterest.level) AS level,closeCompany.name AS company  , "
                    "cpInterest.interestName order BY TOINT( cpInterest.level) DESC",
                    name1 = nameF, name2 = nameL)

    #get ranking order
    table = {}
    for index in result:
        if table.has_key(index.Personid):
            table[index.Personid] = table[index.Personid] + index.level
        else:
            table[index.Personid] = index.level

    #put inorder
    sortedarray = table.items()
    sortedarray.sort(None, key = lambda x: x[1], reverse = False)



    #read data out
    while sortedarray.__len__() != 0:
       current = sortedarray.pop()

       #print common interest for a certain person
       for x in result:

            if list.__contains__(x[5]):
               continue

           #formatting output
            if (current[0] == x[2]) :

                if(not namelist.__contains__(current[0])):
                    fullname ="Name: "
                    fullname =fullname + x[0] + " " + x[1] + ", Working at:"+ x[4] +" id: "
                    print fullname, current[0]

                print "Common Interest: ", x[5], ", At level", x[3]
                namelist.add(current[0])
                list.add(x[5])

       list = set()
       namelist = set() # reset the interest set
       print '\n'




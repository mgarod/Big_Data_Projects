from py2neo import Graph

graph = Graph()  # Makes connection to http://127.0.0.1:7474


def query2():
    # Get nameF, nameL within this function

    list = set()
    namelist = set()


    #get the resource data from the query
    result = graph.cypher.execute(
                    "Match (person:Person {Fname: upper({name1}), Lname: upper({name2})})-[r:Work_at]->(job) "
                    "Match (job)-[r1:DISTANCE]-(closeCompany) where r1.closeby = true "
                    "Match (closePerson)-[:Work_at]->(closeCompany) "
                    "Match (closePerson)-[ri:interested_in]->(cpInterest)<-[:interested_in]-(person) "
                    "return closePerson.Fname AS PersonF, closePerson.Lname AS PersonL"
                    ", closePerson.User_id AS Personid, TOINT(cpInterest.level) AS level,closeCompany.name AS company  , "
                    "cpInterest.interestName order BY TOINT( cpInterest.level) DESC",
                    name1 = nameF, name2 = nameL)


    #return false if person is not found in database
    if not PersonExist(nameF, nameL):
        print 'This person does not exist in Database.'
        return

    if result.__len__() == 0:
        print 'No results found for'+ nameF+ " "+ nameL
        return


    #put inorder
    sortedarray = getRankOrder(result)
    sortedarray.sort(None, key = lambda x: x[1], reverse = False)
    printData(sortedarray, list, namelist, result)


def getRankOrder(result):
    #PreCondition: must be populated result table
    table = {}
    for index in result:
        if table.has_key(index.Personid):
            table[index.Personid] = table[index.Personid] + index.level
        else:
            table[index.Personid] = index.level

    return table.items()



def printData(sortedarray, list, namelist, result):
    #read data out
    while sortedarray.__len__() != 0:
       current = sortedarray.pop()

       #print common interest for a certain person
       for x in result:

            if list.__contains__(x[5]):
               continue

           #find p in x# namelist = set() # reset the interest set
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


def PersonExist(Fname, Lname):

    result = graph.cypher.execute(
        "MATCH (p:Person{Fname: upper({first_name}), Lname: upper({last_name}) } ) return p",
        first_name = Fname, last_name = Lname )

    if result.__len__() > 0:
        return True
    else:
        return False






from py2neo import Graph
import sys
import Validators as valid
import Query3


def query1():
    # Get nameF, nameL within this function
    u_id = valid.validate_num("$ Enter a user_id: ")

    # User Cassandra to verify existence for speed
    if not Query3.user_exists(u_id):
        print "User ID {} was not found".format(u_id)
        return

    graph = Graph()  # Makes connection to http://127.0.0.1:7474
    cypher = graph.cypher
    #get the resource data from the query
    result = cypher.execute(
        """
        Match (person:Person {User_id: {uid}})-[r:Work_at]->(job)
        Match (job)-[r1:DISTANCE]-(closeCompany) where r1.closeby = true
        Match (closePerson)-[:Work_at]->(closeCompany)
        Match (closePerson)-[ri:interested_in]->(cpInterest)<-[ri2:interested_in]-(person)
        return closePerson.Fname AS PersonF, closePerson.Lname AS PersonL
        , closePerson.User_id AS Personid,
        TOINT(ri2.level) AS level,closeCompany.name AS company,ri.level AS OtherLevel,
        cpInterest.interestName order BY TOINT( ri2.level) DESC
        UNION
        Match (person:Person {User_id: {uid}})-[r:Work_at]->(closeCompany)
        Match (closePerson)-[:Work_at]->(closeCompany)
        Match (closePerson)-[ri:interested_in]->(cpInterest)<-[ri2:interested_in]-(person)
        return closePerson.Fname AS PersonF, closePerson.Lname AS PersonL
        , closePerson.User_id AS Personid, TOINT(ri2.level) AS level,closeCompany.name AS company,
        ri.level AS OtherLevel, cpInterest.interestName order BY TOINT( ri2.level) DESC
        """,uid=u_id)
    person = cypher.execute(
        """
        Match (person:Person {User_id: {uid}})- [r:interested_in]->(likes)
        return person.User_id AS ids, person.Fname AS fname, person.Lname AS lname, r.level AS level,
         likes.interestName AS iName
        """
        ,uid=u_id)
    sys.stdout.flush()


    print '\n'


    newArray = getRankOrder(result)
    printPerson(person)

    if result.__len__() == 0 :
        print 'No results were found'
        return

    printData(newArray,result)


def printData(sortedarray, result):
    """
    Print data out
    :param sortedarray: Sorted data
    :param result: table of information
    :return: nothing
    """
   # mylist = set()
    namelist = set()
    i = 1
    #read data out
    while sortedarray.__len__() != 0:
        print 'Rank #',i,'|',
        print '#'*54
        current = sortedarray.pop()
        #print common interest for a certain person
        for x in result:
                #find p in x# namelist = set() # reset the interest set
            if (current[0] == x[2]) :
                if(not namelist.__contains__(current[0])):
                    fullname ="Name: "
                    fullname =fullname + x[0] + " " + x[1] + ", Working at: "+ x[4] +",  User id: "
                    print fullname, current[0]

                print "Common Interest: {} ({}) ".format( x[6], x[5])

                namelist.add(current[0])
        namelist = set() # reset the interest set
        i+=1
        print 'Total shared interest weight: {}'.format(current[1])
        print '\n'


def getRankOrder(result):
    """
    Precondition result must be populated
    Get order of the search
    :param result: table of information
    :return: sorted array with data in order
    """
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
    return sortedarray


def printPerson(person):
    """
    Print person who we are running query on
    :param person:  user entered person
    :return: nothing
    """
    print 'You have entered: '
    k = 0
    print '_'*65
    for i in person:
        if k == 0 :
             print "User id: {} , Name: {} {}".format(i[0],i[1], i[2]),
             print 'who is interest in:'
             k+=1


        print "{:5} | {} ({})".format("",i[4], i[3], width=15)
    print '-'*65




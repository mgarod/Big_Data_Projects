import Validators as valid
import pprint
from cassandra.cluster import Cluster
from cassandra.query import dict_factory

cluster = Cluster()
session = cluster.connect("local")
session.row_factory = dict_factory


def query3():
    """
    Retrieve all information related to a single user
    :return:
    """
    u_id = valid.validate_num("$ Enter a user_id: ")

    if user_exists(u_id):
        result = select_user(u_id)
        print_result(result)
    else:
        print "User ID {} was not found".format(u_id)


def user_exists(uid):
    """
    Precondition: uid is a non-negative integer
    Precondition: There is only 1 record with a given user_id (primary key)

    :param uid: User input user_id
    :return: Boolean
    """
    statement = session.prepare(
        """
        SELECT count(*)
        FROM users
        WHERE user_id = ?
        """
    )
    statement = statement.bind([uid])
    result = session.execute(statement)

    if result[0]["count"] == 1:
        return True
    else:
        return False


def select_user(uid):
    """
    Precondition: There exists exactly one record with the given user_id

    :param uid: User input user_id
    """
    statement = session.prepare(
        """
        SELECT*
        FROM users
        WHERE user_id = ?
        """
    )
    statement = statement.bind([uid])

    result = session.execute(statement)
    return result


def print_result(result):
    uid = result[0]["user_id"]
    fn = result[0]["first_name"].encode("ascii")
    ln = result[0]["last_name"].encode("ascii")
    orgname = result[0]["organization"].encode("ascii")
    orgtype = result[0]["organization_type"].encode("ascii")
    projs = projs_to_str(result[0]["projects"])
    interests = map_to_list(result[0]["interests"])
    skills = map_to_list(result[0]["skills"])

    print
    print "{:15} | {}".format("Column", "Value", width=15)
    print "-"*50
    print "{:15} | {}".format("User ID (P.Key)", str(uid), width=15)
    print "-"*50
    print "{:15} | {}".format("First Name", fn, width=15)
    print "-"*50
    print "{:15} | {}".format("Last Name", ln, width=15)
    print "-"*50
    print "{:15} | {}".format("Organization", orgname, width=15)
    print "-"*50
    print "{:15} | {}".format("Org. Type", orgtype, width=15)
    print "-"*50
    print "{:15} | {}".format("Projects", projs, width=15)
    print "-"*50
    print_list(skills, "Skills")
    print "-"*50
    print_list(interests, "Interests")


def projs_to_str(l):
    a_str = ""
    for i in l:
        a_str += i.encode("ascii")+" "
    return a_str


def map_to_list(interests_map):
    l = []
    for name in interests_map.keys():
        num = interests_map[name]
        l.append(name.encode("ascii") + " (" + str(num) + ")")
    return l


def print_list(interest_list, i_type):
    print "{:15} | {:25}".format(i_type, interest_list[0], width=15)

    i = 1
    while i < len(interest_list):
        print "{:15} | {}".format("", interest_list[i], width=15)
        i += 1

from cassandra.cluster import Cluster
from cassandra.query import dict_factory, named_tuple_factory

def print_table_tuple():
    session.row_factory = named_tuple_factory
    # Select will return a set of namedtuple
    rows = session.execute("SELECT * FROM users")
    for name, year, sex, interests in rows:
        print "{}, {}, {}, {}".format(name, year, sex, interests)
        # Printing doesn't make sense because the schema gets rearranged

def print_table_map():
    session.row_factory = dict_factory
    # We can alter the query to return a dictionary instead of tuples
    rows = session.execute("SELECT * FROM users")
    for row in rows:
        print "{}, {}, {}, {}".format(
            row["name"], row["year"], row["sex"], row["interests"])
        # With a map, we can control the order in which the data is shown

cluster = Cluster()
session = cluster.connect()
session.execute(
    """
    CREATE KEYSPACE IF NOT EXISTS local
    WITH replication = {'class':'SimpleStrategy', 'replication_factor': 1}
    """
)
# Replication factor must be set to 1 because we are working on only 1 node
session.set_keyspace("local")

# Prepare a statement to execute - Drop Table
drop_table_stmt = session.prepare(
    """

    DROP TABLE IF EXISTS users
    """
)
session.execute(drop_table_stmt)

# Prepare a statement to execute - Create Table
create_table_stmt = session.prepare(
    """
    CREATE TABLE IF NOT EXISTS users
    (name text PRIMARY KEY, year int, sex text, interests list<text>)
    """
)
session.execute(create_table_stmt)

# Prepare and bind a statement to execute - Insert
insert_stmt = session.prepare(
    """
    INSERT INTO users (name, year, sex, interests)
    VALUES (?, ?, ?, ?) IF NOT EXISTS
    """
)
bound_insert_stmt = insert_stmt.bind(
    ['Jones', 2001, 'Male', ['Wine', 'Music', 'Movies']]
)
session.execute(bound_insert_stmt)
print_table_tuple()

# Append an interest 'Sailing' to Jones
update_stmt = session.prepare(
    """
    UPDATE users
    SET interests = interests + [ 'Sailing' ] WHERE name = 'Jones';
    """
)
session.execute(update_stmt)
print_table_map()

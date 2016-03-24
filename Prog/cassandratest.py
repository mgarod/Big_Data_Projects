from cassandra.cluster import Cluster
from cassandra.query import dict_factory

cluster = Cluster()
session = cluster.connect()
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

# Select will return a set of namedtuple
rows = session.execute("SELECT * FROM users")
for name, year, sex, interests in rows:
    print "{}, {}, {}, {}".format(name, year, sex, interests)
    # Printing doesn't make sense because the schema gets rearranged

# We can alter the query to return a dictionary instead of tuples
session.row_factory = dict_factory
rows = session.execute("SELECT * FROM users")
for row in rows:
    print "{}, {}, {}, {}".format(
        row["name"], row["year"], row["sex"], row["interests"])
    # With a map, we can control the order in which the data is shown


# If we want to programmatically insert, create map to insert
# mymap = {
#     "one": "name",
#     "two": 2001,
#     "three": "female",
# }

# Usage: session.execute(triple_quote with print formatting , variables)
# session.execute(
#     """
#     INSERT INTO users
#       (user_name, birth_year, gender)
#     VALUES (%(one)s, %(two)s, %(three)s)
#     """, mymap)
#

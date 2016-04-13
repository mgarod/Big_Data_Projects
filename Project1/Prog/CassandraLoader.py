import csv
from cassandra.cluster import Cluster
from cassandra.query import dict_factory

cluster = Cluster()
session = cluster.connect()


def load_cassandra():
    setup_session()
    load_names()
    load_orgs()
    load_projects()
    load_skills()
    load_interests()


def setup_session():
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS local
        WITH replication = {'class':'SimpleStrategy', 'replication_factor': 1}
        """
    )
    session.set_keyspace("local")

    session.execute(
        """
        DROP TABLE IF EXISTS users
        """
    )

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS users(
            user_id int PRIMARY KEY,
            first_name text,
            last_name text,
            organization text,
            organization_type text,
            projects set<text>,
            skills map<text, int>,
            interests map<text, int>
        )
        """
    )


def load_names():
    insert_stmt = session.prepare(
        """
        INSERT INTO users (user_id, first_name, last_name)
        VALUES (?, ?, ?) IF NOT EXISTS
        """
    )

    with open('Data/names.csv', 'r') as f:
        csvfile = csv.reader(f)
        next(csvfile, None)  # Skip headers
        for row in csvfile:
            insert_stmt = insert_stmt.bind([int(row[0]), row[1], row[2]])
            session.execute(insert_stmt)


def load_orgs():
    update_stmt = session.prepare(
        """
        UPDATE users
        SET organization = ?, organization_type = ?
        WHERE user_id = ?
        """
    )

    with open('Data/orgs.csv', 'r') as f:
        csvfile = csv.reader(f)
        next(csvfile, None)  # Skip headers
        for row in csvfile:
            update_stmt = update_stmt.bind([row[1], row[2], int(row[0])])
            session.execute(update_stmt)


def load_projects():
    update_stmt = session.prepare(
        """
        UPDATE users
        SET projects = projects + ?
        WHERE user_id = ?
        """
    )

    with open('Data/projects.csv', 'r') as f:
        csvfile = csv.reader(f)
        next(csvfile, None)  # Skip headers
        for row in csvfile:
            update_stmt = update_stmt.bind([{row[1]}, int(row[0])])
            session.execute(update_stmt)


def load_skills():
    update_stmt = session.prepare(
        """
        UPDATE users
        SET skills[?] = ?
        WHERE user_id = ?
        """
    )

    with open('Data/skills.csv', 'r') as f:
        csvfile = csv.reader(f)
        next(csvfile, None)  # Skip headers
        for row in csvfile:
            update_stmt = update_stmt.bind([row[1], int(row[2]), int(row[0])])
            session.execute(update_stmt)


def load_interests():
    update_stmt = session.prepare(
        """
        UPDATE users
        SET interests[?] = ?
        WHERE user_id = ?
        """
    )

    with open('Data/interests.csv', 'r') as f:
        csvfile = csv.reader(f)
        next(csvfile, None)  # Skip headers
        for row in csvfile:
            update_stmt = update_stmt.bind([row[1], int(row[2]), int(row[0])])
            session.execute(update_stmt)


def print_table_map():
    session.row_factory = dict_factory
    rows = session.execute("SELECT * FROM users")
    for row in rows:
        print row["interests"]


load_cassandra()

import Query1 as q1
import Query2 as q2
import Query3 as q3
import Validators as valid
import pandas as pd
import CassandraLoader as cload
import Neo4jLoader as nload


class Interface:
    def __init__(self):
        self.Actions = {
            1: q1.query1,
            2: q2.query2,
            3: q3.query3
        }

        cload.load_cassandra()
        nload.load_neo4j()

        while True:
            self.display()

    def display(self):
        commands = ["Execute Query 1",
                    "Execute Query 2",
                    "Execute Query 3"]
        keys = range(1, len(commands)+1)
        df = pd.DataFrame({"Enter Key": keys, "Command List": commands},
                          index=keys)

        print
        print "Enter a number to select a command"
        print "--------------------------------------------------"
        print df.to_string(index=False)

        user_input = valid.validate_command(keys)
        self.Actions[user_input]()

interface = Interface()
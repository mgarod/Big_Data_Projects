#Big Data Project #1 - collaborator.net
CSCI 493.71, Spring 2016

Professor Lei Xie

### Authors
- Michael Garod
- Ryan Kallicharran

### Runtime Instructions
1. Navigate to path/Prog/Data and run 'python DataGenerators.py' (Optional)
2. Navigate to path/Prog and run 'python Main.py'

The program itself expects csv files present in path/Prog/Data folder with certain names (see below).

If you are using other than that produced by the included generator, you must rename the files to those listed below.

#### WARNING
Execution of this program will create keyspace "local", and drop any Cassandra table called "users" in "local".

It will also delete all nodes in your local neo4j database.

The databases are flushed so that data worked on only reflects that of the current run.

### Databases
This project uses Cassandra and Neo4j. The program requires that an instance of each database is running locally before execution.

### CSV Files
If DataGenerators.py is run from the path/Prog/Data folder, then that folder will contain 6 csv files with the following headers listed below with some example data:

names.csv: User_id, first name, last name

orgs.csv: User_id, organization, organization type	

projects.csv: User_id, Project	

skills.csv: User_id, Skill,	Skill level	

interests.csv: User_id, Interest, Interest level	

distance.csv: Organization 1, Organization 2, Distance	

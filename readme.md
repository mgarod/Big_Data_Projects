# Runtime Instructions

1. Navigate to path/Prog/Data
2. Run 'python DataGenerators.py'
3. Navigate to path/Prog
4. Run 'python Main.py'

## Headers
Here are examples of CSV headers. Each line represents a CSV file

User_id, first name, last name

User_id, organization, organization type	

User_id, Project	

User_id, Skill,	Skill level	

User_id, Interest, Interest level	

Organization 1, Organization 2, Distance	

## Necessary DBs 
USER DB (Cassandra)

COLLEAUGE GRAPH (Neo4j)
  * People nodes
  * bidirectional "Worked with" edges
	
COMPANY GRAPH(completed with cypher)
  * Company nodes
  * weighted bidirectional "Distance" edges
  * bidirectional "Less than 10 miles" edges
  * **csv File require header Orangization 1, Organization 2, Distance


## Queries Pseudocode
#### Query 1

1. Get user from USER DB
2. Keep map of user skills and interests, and user company
3. Get all companies <10 miles from COMPANY GRAPH
4. Find all otherusers at companies <10 miles from USER DB
5. Match user to otheruser based on interest/skill intersection
6. Sort otherusers by highest match

#### Query 2

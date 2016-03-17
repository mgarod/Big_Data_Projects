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
	
COMPANY GRAPH
  * Company nodes
  * weighted bidirectional "Distance" edges
  * bidirectional "Less than 10 miles" edges


## Queries Pseudocode
#### Query 1

1. Get user from USER DB
2. Keep map of user skills and interests, and user company
3. Get all companies <10 miles from COMPANY GRAPH
4. Find all otherusers at companies <10 miles from USER DB
5. Match user to otheruser based on interest/skill intersection
6. Sort otherusers by highest match

#### Query 2
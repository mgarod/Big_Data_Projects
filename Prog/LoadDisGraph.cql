CREATE CONSTRAINT ON (o:Organization) ASSERT o.name IS UNIQUE;



//**1st iteration
LOAD CSV WITH HEADERS FROM 
//case sensitive for file path
'file:////home//ryan/Desktop/BigData/code/distance.csv' AS line 

//creating nodes
MERGE (o:Organization { name: line.`Organization 1`, name: line.`Organization 2` } )
;


//**2nd iteration
LOAD CSV WITH HEADERS FROM 
//case sensitive for file path
'file:////home//ryan/Desktop/BigData/code/distance.csv' AS line 

//find company1 in csv
MATCH (company1:Organization { name: line.`Organization 1`})  
//find company2 in csv
MATCH (company2:Organization { name: line.`Organization 2`})  
//create our relationship
MERGE (company1)-[m:DISTANCE]-(company2)

SET m.miles = TOINT(line.Distance),
	m.closeby = CASE WHEN TOINT(line.Distance) < 10 THEN true ELSE false END;


//for query  example match(a {name: 'Twitter'} )-[r]-(b) where r.closeby = true return b	






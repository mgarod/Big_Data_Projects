"""
You must run this file with /usr/local/bin/spark-submit
"""
from pyspark import SparkConf, SparkContext
import sys

if len(sys.argv) != 2:
        print "Usage: spark-submit pysparktest.py <file>"
        exit(-1)


conf = (SparkConf()
         .setMaster("local")
         #.setAppName("My app")
         #.set("spark.executor.memory", "4g")
)
sc = SparkContext(conf = conf)
###########################################################
# WORD FREQUENCY BY DOCUMENT
# (Key, Value) ---> ( tuple (docid, word), 1) )

def docid_word(x):
    l = list()
    doc_id = x[0]
    for word in x[1].split():
        tup = ((doc_id, word), 1)
        l.append(tup)
    return l

# ex = sc.textFile("example.txt")
ex = sc.textFile(sys.argv[1])  # Open the file, splitting on '\n'
ex2 = ex.map(lambda x: x.split(' ', 1))  # Separate the doc id
ex3 = ex2.map(docid_word)  # Generate (docid, word) ---> 1
flat = ex3.flatMap(lambda x: x)  # Remove partitions separating documents
grouped = flat.reduceByKey(lambda x,y: x+y)  # Group By Key, sum frequency
# (Key, Value) ---> ( tuple (docid, word), docfreq) ) 

print "Word Count Per Document (sorted by doc, then frequency):"
sortbyfreq = grouped.sortBy(lambda x: x[1], ascending=False)
sortbydoc = sortbyfreq.sortBy(lambda x: x[0][0])
print sortbydoc.collect()

###########################################################
# TOTAL FREQUENCY IN CORPUS
# (Key, Value) ---> ( word, freq in that document )

total = grouped.map(lambda x: (x[0][1], x[1]))  # Extract tuple
total2 = total.reduceByKey(lambda x,y: x+y)  # Total freq in all documents

print "\nWord Count for the Corpus (sorted by frequency):"
print total2.sortBy(lambda x: x[1], ascending=False).collect()

###########################################################
# TERM FREQUENCY RATIO
# We must have the total number of documents
total_docs = int(grouped.map(lambda x: x[0][0]).max())

def duplicate(x, num_docs):
	l = list()
	for i in range(1, num_docs+1):
		tup = ((str(i), x[0]), x[1])
		l.append(tup)
	return l

def get_ratio(x):
	# The values of n1, n2 are not guaranteed to be in any order
	n1, n2 = x[1]
	return (x[0], float(min(n1,n2)) / float(max(n1,n2)))

# Make the following for EVERY VALUE of docid
# (Key, Value) ---> ( tuple(docid, word) , totalfreq )
duplicatedfreq = total2.map(lambda x: duplicate(x, total_docs))
flattotalfreq = duplicatedfreq.flatMap(lambda x: x)

# Join is an inner join which collects values into tuples only where
#  there is a match on keys between the two RDDs. Recall that 'grouped'
#  is an RDD of (key, value) ---> ((docid, word), docfreq)
joined = flattotalfreq.join(grouped)

# Every tuple of key:(docid, word) now has a 2-tuple (docfreq, totalfreq)
#  not necessarily in that order i.e. it could be (totalfreq, docfreq)
termfrequency = joined.map(get_ratio)

print "\nTerm Frequency (sorted by docid, then frequency):"
tfbyfreq = termfrequency.sortBy(lambda x: x[1], ascending=False)
tfbydoc = tfbyfreq.sortBy(lambda x: x[0][0])
print tfbydoc.collect()








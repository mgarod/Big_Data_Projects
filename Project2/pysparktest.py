"""
You must run this file with /usr/local/bin/spark-submit
"""
from pyspark import SparkConf, SparkContext
import sys
import math
from operator import add

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
fullfile = sc.textFile(sys.argv[1])  # Open the file, splitting on '\n'
splitid = fullfile.map(lambda x: x.split(' ', 1))  # Separate the doc id
id_word_tuples = splitid.map(docid_word)  # Generate (docid, word) ---> 1
flat = id_word_tuples.flatMap(lambda x: x)  # Remove partitions separating documents
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
# Make 
total_docs = grouped.groupBy(lambda x: x[0][0]).count()

def duplicatefreq(x, num_docs):
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
duplicatedfreq = total2.map(lambda x: duplicatefreq(x, total_docs))
flattotalfreq = duplicatedfreq.flatMap(lambda x: x)

# Join is an inner join which collects values into tuples only where
#  there is a match on keys between the two RDDs. Recall that 'grouped'
#  is an RDD of (key, value) ---> ((docid, word), docfreq)
joined = flattotalfreq.join(grouped)

# Every tuple of key:(docid, word) now has a 2-tuple (docfreq, totalfreq)
#  not necessarily in that order i.e. it could be (totalfreq, docfreq)
termfrequency = joined.map(get_ratio)

print "\nTerm Frequency (sorted by docid, then frequency ratio):"
tfbyfreq = termfrequency.sortBy(lambda x: x[1], ascending=False)
tfbydoc = tfbyfreq.sortBy(lambda x: x[0][0])
print tfbydoc.collect()

###########################################################
# INVERSE DOCUMENT FREQUENCY
# We use total_docs from the previous calculation
# math.log(x) returns the natural logarithm of x

def idf(x):
	return (x[0], math.log(float(total_docs) / float(x[1])))

# Emit (word, 1) representing this word appears in 1 document
# There is no duplicates b/c we are starting from WORD FREQ. BY DOC. result
words_1 = grouped.map(lambda x: (x[0][1], 1))
num_docs_with_word = words_1.groupByKey().map(lambda x: (x[0], sum(x[1])))
inversedocfreq = num_docs_with_word.map(idf)
idfbyidf = inversedocfreq.sortBy(lambda x: x[1])

print "\nInverse Document Frequency (sorted by idf):"
print idfbyidf.collect()


###########################################################
# correct term frequency
# total2 contains (word, freq) among the entire corpus
word_count = float(total2.map(lambda x: x[1]).reduce(add))

def tf_ratio_tuple(x):
	return (x[0], float(x[1]) / word_count)

tf = total2.map(tf_ratio_tuple)
print "\nTerm Frequency (sorted by frequency ratio):"
print tf.sortBy(lambda x: x[1], ascending=False).collect()






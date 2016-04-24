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

def docid_word(x):
    l = list()
    doc_id = x[0]
    for word in x[1].split():
        tuple = ((doc_id, word), 1)
        l.append(tuple)
    return l

# (Key, Value) ---> ( tuple (docid, word), 1) ) 
# ex = sc.textFile("example.txt")
ex = sc.textFile(sys.argv[1])  # Open the file, splitting on '\n'
ex2 = ex.map(lambda x: x.split(' ', 1))  # Separate the doc id
ex3 = ex2.map(docid_word)  # Generate (docid, word) ---> 1
flat = ex3.flatMap(lambda x: x)  # Remove partitions separating documents
grouped = flat.reduceByKey(lambda x,y: x+y)  # Group By Key, sum frequency

print "Word Count Per Document (sorted by doc, then frequency):"
sortbyfreq = grouped.sortBy(lambda x: x[1], ascending=False)
sortbydoc = sortbyfreq.sortBy(lambda x: x[0][0])
print sortbydoc.collect()

###########################################################

# (Key Value) ---> ( word, 1 )
total = grouped.map(lambda x: (x[0][1], x[1]))  # Extract only (word, freq)
total2 = total.reduceByKey(lambda x,y: x+y)  # Traditional word count MapReduce

print "\nWord Count for the Corpus (sorted by frequency):"
print total2.sortBy(lambda x: x[1], ascending=False).collect()

###########################################################

















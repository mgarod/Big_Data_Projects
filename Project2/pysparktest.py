"""
You can run this file with root/.../bin/spark-submit
"""
from pyspark import SparkConf, SparkContext


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
ex = sc.textFile("example.txt")  # Open the file, splitting on '\n'
ex2 = ex.map(lambda x: x.split('\t'))  # Separate the doc id
ex3 = ex2.map(docid_word)  # Generate (docid, word) ---> 1
flat = ex3.flatMap(lambda x: x)  # Remove partitions separating documents
grouped = flat.reduceByKey(lambda x,y: x+y)  # Group By Key, sum frequency

print "Word Count Per Document (sorted by frequency):"
print grouped.sortBy(lambda x: x[1], ascending=False).collect()

# (Key Value) ---> ( word, 1 )
total = grouped.map(lambda x: (x[0][1], 1))  # Extract only (word, 1)
total2 = total.reduceByKey(lambda x,y: x+y)  # Traditional word count MapReduce

print "\nWord Count for the Courpus (sorted by frequency):"
print total2.sortBy(lambda x: x[1], ascending=False).collect()
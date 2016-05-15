"""
You must run this file with /usr/local/bin/spark-submit
"""
from pyspark import SparkConf, SparkContext
import sys


# if len(sys.argv) != 2:
#         print "Usage: spark-submit pysparktest.py <file>"
#         exit(-1)


conf = (SparkConf()
         .setMaster("local")
         #.setAppName("My app")
         #.set("spark.executor.memory", "4g")
)
sc = SparkContext(conf=conf)

def pretty_printer(x):
    """
    param x: An RDD
    (k,v) can be either (2-tuple, single value) or (single key, single value)
    """
    l = x.collect()
    for i in l:
        print i

###############################################################################
import math

def docid_word(x):
    l = list()
    doc_id = x[0]
    for word in x[1].split():
        if word.startswith("disease_") or word.startswith("gene_"):
            tup = ((doc_id, word), 1)
            l.append(tup)
    return l

fullfile = sc.textFile("project2_data.txt")
#fullfile = sc.textFile(sys.argv[1])  # Open the file, splitting on '\n'
splitid = fullfile.map(lambda x: x.split(' ', 1))  # Separate the doc id
id_word_tuples = splitid.map(docid_word)  # Generate (docid, word) ---> 1
flat = id_word_tuples.flatMap(lambda x: x)  # Remove partitions separating documents
grouped = flat.reduceByKey(lambda x,y: x+y)  # Group By Key, sum frequency

# END RESULT: ((docid, termid), term_freq_in_doc)

###############################################################################

# Make (termid, (term_freq_in_doc, [doc_id]))
corpus_count = grouped.map(lambda x: (x[0][1], (x[1], [x[0][0]])))

# Make (termid, (term_freq_in_corpus, [all_docs_containing_termid]))
# Key = termid
# Value = (tf_in_cor, [list])
corpus_freq = corpus_count.reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))

###############################################################################
# Unwind the doc list to make ((doc, term), calc_value) tuples for every doc
# some_value will either be corpus_frequency, or idf
# This output format will be standard for reducing
def generate(x):
    term = x[0]
    some_value = x[1][0]
    l = list()
    for doc in x[1][1]:
        l.append(((doc, term), some_value))
    return l


a = corpus_freq.map(generate)

# We have an RDD of lists of tuples. Flatten to have RDD of tuples only
b = a.flatMap(lambda x: x)

# Inner join keys with grouped to get ((doc, term), (doc_freq, corpus_freq))
c = grouped.join(b)


def calc_tf(x):
    # tf is term freq, cf is corpus frequency
    tf, cf = x[1]
    return (x[0], float(tf) / float(cf))

# termfreq is now in form ((docid, termid), termfreq)
termfreq = c.map(calc_tf)

###############################################################################
# Global Variable, |D|
NUM_DOCS = grouped.map(lambda x: x[0][0]).distinct().count()

def calc_idf(x, n):
    # NUM_DOCS is a global variable (we could make it a parameter)
    return (x[0], ((math.log(float(n) / float(x[1][0]))), x[1][1]))

# Make (termid, (1 doc appearance, [docid])) for every item in grouped
trivial_doc_count = grouped.map(lambda x: (x[0][1], (1, [x[0][0]])))

# Make (termid, (sum of doc appearance, [docid 1, ... , docid n]))
num_docs_per_term = trivial_doc_count.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))

# Make (termid, (idf, [docid 1, ... , docid n]))
idf_with_doc_list = num_docs_per_term.map(lambda x: calc_idf(x, NUM_DOCS))

# idf is now in form ((docid, termid), termfreq)
idf_unflattened = idf_with_doc_list.map(generate)

idf = idf_unflattened.flatMap(lambda x: x).filter(lambda x: x[1] > 0)

###############################################################################

# Make tfidf matrix as ((docid, termid), tfidf)
#tfidf_temp = idf.join(termfreq)
#tfidf = tfidf_temp.mapValues(lambda x: x[0]*x[1])

# This sequence is far more efficient than the join above
tfidf_temp = idf.union(termfreq)
tfidf = tfidf_temp.reduceByKey(lambda x,y: x*y)
tfidf = tfidf.filter(lambda x: x[1] != 0)
tfidf.cache()

###############################################################################
# Find similarity of "t3" to all other terms
#queryterm = "t2"
queryterm = "gene_nmdars_gene"

# The tfidf matrix as ((docid, termid), tfidf)
queryfilter = tfidf.filter(lambda x: x[0][1] == queryterm)
otherfilter = tfidf.filter(lambda x: x[0][1] != queryterm)

# Filtering beforehand prevents duplicates
numer_cart = queryfilter.cartesian(otherfilter)

# If docid matches docid, then do the multiply for the other termid
def dotprod(x):
    if x[0][0][0]==x[1][0][0]:
        return (x[1][0][1], x[0][1]*x[1][1])

# vectormult is (othertermid, query_idf*other_idf)
vectormult = numer_cart.map(dotprod).filter(lambda x: x is not None)
numerators = vectormult.reduceByKey(lambda x,y: x+y)

###############################################################################

# Square every element of the ifidf matrix as (termid, idf^2)
v_squared = tfidf.map(lambda x: (x[0][1], x[1]*x[1]))
# Sum every element associated a term as (termid, Sigma(idf^2))
v_sum = v_squared.reduceByKey(lambda x, y: x+y)
# Root every sum of a term as (term, sqrt(Sigma(idf^2))
v_root = v_sum.mapValues(lambda x: math.sqrt(x))
# v_root is (termid, magnitude of vector)

# Separate the query vector from all other vectors
query_denom = v_root.filter(lambda x: x[0] == queryterm)
other_denom = v_root.filter(lambda x: x[0] != queryterm)

# Bring the query vector to all other vectors
denom_cart = query_denom.cartesian(other_denom)
# Multiply the vectors as (othertermid, ||A||*||B|| )
denominators = denom_cart.map(lambda x: (x[1][0], (x[0][1]*x[1][1])))

# Inner join all numerators to their matching denominators
fractions = numerators.join(denominators)

# Complete the division of the fraction
division = fractions.map(lambda x: (x[0], x[1][0]/x[1][1]))
similarity = division.sortBy(lambda x: x[1], ascending=False)
pretty_printer(similarity)
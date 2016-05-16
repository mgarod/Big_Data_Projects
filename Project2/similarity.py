"""
You must run this file with /usr/local/bin/spark-submit
"""
from pyspark import SparkConf, SparkContext
import sys

import boto
from boto.s3.key import Key


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

conn = boto.connect_s3('AKIAJDU7XXQHS3IKN27Q','qDOmwl/37w2acYu3OmsyR7OWkVL4lq+fQddfgRZX')
bucket = conn.get_bucket('fizzbuzz')
k = Key(bucket)
k.key = 'project2_data.txt'
dataFile = k.key

# The file is being read from Amazon S3, but the process is run locally
fullfile = sc.textFile(dataFile)

# Use the following if you want to read the data locally as well
# fullfile = sc.textFile('project2_data.txt')

# Separate the doc id
splitid = fullfile.map(lambda x: x.split(' ', 1))

# Generate a list of ((docid, term), 1) per document
id_word_tuples = splitid.map(docid_word)

# Remove list partitions. We now have many pairs like ((docid, term), 1)
flat = id_word_tuples.flatMap(lambda x: x)

# Reduce by key, to get term frequency per document
grouped = flat.reduceByKey(lambda x,y: x+y)  # Group By Key, sum frequency

# END RESULT: ((docid, term), term_freq_in_doc)

###############################################################################

# Make (term, (term_freq_in_doc, [doc_id]))
corpus_count = grouped.map(lambda x: (x[0][1], (x[1], [x[0][0]])))

# Key = term, Value = (tf_in_cor, [list])
corpus_freq = corpus_count.reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))

# END RESULT: (term, (term_corpus_freq, [all_docs_containing_term]))

###############################################################################
def generate(x):
    """
    Unwind the doc list to make ((doc, term), calc_value) tuples for every doc
    some_value will either be corpus_frequency, or idf
    This output format will be standard for reducing, and used multiple times
    """
    term = x[0]
    some_value = x[1][0]
    l = list()
    for doc in x[1][1]:
        l.append(((doc, term), some_value))
    return l

# Unwind corpus_freq, to ((docid, term), corpus_freq)
all_corpus_freq = corpus_freq.map(generate)

# We have an RDD of lists of tuples. Flatten to have RDD of tuples only
flat_all_corpus_freq = all_corpus_freq.flatMap(lambda x: x)

# Inner join keys with grouped to get ((doc, term), (doc_freq, corpus_freq))
c = grouped.join(flat_all_corpus_freq)

# This function will produce ((doc, term), (doc_freq / corpus_freq))
def calc_tf(x):
    # x[1][0] is doc freq, x[1][1] is corpus frequency
    return (x[0], float(x[1][0]) / float(x[1][1]))


termfreq = c.map(calc_tf)

# END RESULT: ((docid, term), termfreq)

###############################################################################
# Global Variable, |D|
# Using previous results, emit all docids, then find distinct and count
NUM_DOCS = grouped.map(lambda x: x[0][0]).distinct().count()

def calc_idf(x, n):
    """
    :param x: The element of an RDD of the form
        (term, (idf, [docid 1, ... , docid n]))
    :param n: The total number of documents in the corpus
    """
    return (x[0], ((math.log(float(n) / float(x[1][0]))), x[1][1]))

# Make (term, (1 doc appearance, [docid])) for every item in grouped
#   Since we're using ((docid, term), term_freq_in_doc), we have the unique
#   terms per document
trivial_doc_count = grouped.map(lambda x: (x[0][1], (1, [x[0][0]])))

# Make (term, (sum of doc appearance, [docid 1, ... , docid n]))
num_docs_per_term = trivial_doc_count.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))

# Make (term, (idf, [docid 1, ... , docid n]))
idf_with_doc_list = num_docs_per_term.map(lambda x: calc_idf(x, NUM_DOCS))

# Use generate to get closer to the form ((docid, term), idf)
idf_unflattened = idf_with_doc_list.map(generate)

# Flatten the previous for ((docid, term), idf), filtering out where idf == 0
idf = idf_unflattened.flatMap(lambda x: x).filter(lambda x: x[1] > 0)

###############################################################################

# Make tfidf matrix as ((docid, term), tfidf)
#tfidf_temp = idf.join(termfreq)
#tfidf = tfidf_temp.mapValues(lambda x: x[0]*x[1])

# This sequence is far more efficient than the join above
# Make an RDD with all termfreq and idf
tfidf_temp = idf.union(termfreq)

# We reduce by key since both tf and idf are in the form ((docid, term) value)
tfidf = tfidf_temp.reduceByKey(lambda x,y: x*y)

# Eliminate those tf*idf whose values are 0
tfidf = tfidf.filter(lambda x: x[1] != 0)

# This RDD is accesses frequently, so cache it for improved performance
tfidf.cache()

# END RESULT: ((docid, term), tfidf)

###############################################################################
# This section creates the numerators of the cosine similiarity function

# Search for this specific query
queryterm = "gene_nmdars_gene"

# Make an RDD with only those tfidf of the query
queryfilter = tfidf.filter(lambda x: x[0][1] == queryterm)

# Make an RDD with only those tfidf associated with diseases
#   Since the query is a gene in this case, we are guaranteed not compare
#   the gene to itself
otherfilter = tfidf.filter(lambda x: x[0][1].startswith('disease_'))

# Filtering beforehand prevents duplicates and reduces computations
# This will make all combos in the form:
#    (((docid, queryterm), query_tfidf), ((docid, otherterm), other_tfidf))
numer_cart = queryfilter.cartesian(otherfilter)


def dotprod(x):
    # If docids match, then do the multiply their tfidfs
    if x[0][0][0]==x[1][0][0]:
        return (x[1][0][1], x[0][1]*x[1][1])

# vectormult is (otherterm, query_idf*other_idf)
vectormult = numer_cart.map(dotprod).filter(lambda x: x is not None)

# Sum all products associated with the otherterm key
numerators = vectormult.reduceByKey(lambda x,y: x+y)

# END RESULT: (otherterm, dotproduct_of_query_and_other)

###############################################################################
# This section creates the denominators of the cosine similarity function

# Square every element of the ifidf matrix as (term, idf^2)
#   (docid is not needed in this calculation)
v_squared = tfidf.map(lambda x: (x[0][1], x[1]*x[1]))

# Sum every element associated a term as (term, sum(idf^2))
v_sum = v_squared.reduceByKey(lambda x, y: x+y)

# Root every sum of a term as (term, sqrt(sum(idf^2)))
v_root = v_sum.mapValues(lambda x: math.sqrt(x))
# v_root is now: (term, magnitude of vector)

# Separate the query vector, and the disease vectors
query_denom = v_root.filter(lambda x: x[0] == queryterm)
other_denom = v_root.filter(lambda x: x[0].startswith('disease_'))

# Make ((queryterm, ||query||), (otherterm, ||other||))
denom_cart = query_denom.cartesian(other_denom)

# Multiply the vectors as (otherterm, ||query||*||other|| )
denominators = denom_cart.map(lambda x: (x[1][0], (x[0][1]*x[1][1])))

# END RESULT: (otherterm, product_of_||query||_and_||other||)

###############################################################################

# Inner join all numerators to their matching denominators
#   in the form ((otherterm, dotproduct), (otherterm, product_of_magnitudes))
fractions = numerators.join(denominators)

# Make (otherterm, dotproduct / product_of_magnitudes)
division = fractions.map(lambda x: (x[0], x[1][0]/x[1][1]))

# Sort descending
similarity = division.sortBy(lambda x: x[1], ascending=False)

# Print to std::out, where each element is has a new line
pretty_printer(similarity)
















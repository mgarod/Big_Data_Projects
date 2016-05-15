"""
This test script counts the number of numbers (exclusively) divisible
by 3, 5, and 15 from 0 to 1,000,000.
You can simply copy and paste the following into the PySpark interpreter.
"""
from pyspark import SparkConf, SparkContext
import sys
import psutil


conf = (SparkConf()
         .setMaster("local")
         #.setAppName("My app")
         #.set("spark.executor.memory", "4g")
)
sc = SparkContext(conf=conf)


def fizzbuzz(x):
    if (x % 3) == 0 and (x % 5) == 0:
        return 15
    elif (x % 5) == 0:
        return 5
    elif (x % 3) == 0:
        return 3


def isInt(x):
    try:
        return int(x)
    except:
        return False


def tuplefactory(x):
    return (x, 1)


def sum_list(x):
    return (x[0], sum(x[1]))


a = sc.parallelize(range(1, 1000000))
b = a.map(fizzbuzz)
c = b.filter(isInt)
d = c.map(tuplefactory)
e = d.groupByKey()
f = e.map(sum_list)
f.collect()

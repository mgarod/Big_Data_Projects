"""
This test script counts the number of numbers (exclusively) divisible
by 3, 5, and 15 from 0 to 1,000,000.

You can simply copy and paste the following into the PySpark interpreter.
"""

import psutil


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

#################################

lines = sc.textFile("sherlock.txt")
lines = lines.filter(lambda x: len(x) > 0)
splitlines = lines.map(lambda x: x.split())

lines = lines.flatMap()


def counter(x):
    l = list()
    for i in x:
        l.append((i, 1))
    return l


lines = lines.map(counter)

##################################
lines = list()

with open("sherlock.txt") as f:
    for line in f:
        line = line.split()
        line = sc.parallelize(line)
        lines.append(line)

for i in range(len(lines)):
    lines[i] = lines[i].map(lambda x: (x, 1))

for i in range(len(lines)):
    lines[i] = lines[i].groupByKey().map(lambda x: (x[0], sum(list(x[1]))))










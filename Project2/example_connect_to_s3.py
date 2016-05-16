###########################################################
#opening and reading from s3
import boto
from boto.s3.key import Key
conn = boto.connect_s3('AKIAJDU7XXQHS3IKN27Q','qDOmwl/37w2acYu3OmsyR7OWkVL4lq+fQddfgRZX')
bucket = conn.get_bucket('fizzbuzz')
k = Key(bucket)
k.key = 'lx_data.txt'
dataFile = k.key
###########################################################
#dataFile = "lx_data.txt"  # Should be some file on your system

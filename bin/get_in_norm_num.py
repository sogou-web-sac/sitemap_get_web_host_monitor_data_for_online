import sys
from urlparse import urlparse
from pyspark import SparkContext

def get_host(url):
  return urlparse(url).hostname

input = sys.argv[1]
output = sys.argv[2]
print input
print output

sc = SparkContext()
rdd = sc.textFile(input, use_unicode=False).cache()
rdd = rdd \
  .filter(lambda row: len(row.strip().split()) >= 3) \
  .map(lambda row: row.strip().split()[2]) \
  .map(lambda x: (get_host(x), 1)) \
  .reduceByKey(lambda acc, n: acc+n)
rdd.repartition(1).saveAsTextFile(output)
sc.stop()

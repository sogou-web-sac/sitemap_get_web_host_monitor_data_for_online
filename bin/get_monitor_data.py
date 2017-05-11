import sys
from urlparse import urlparse
from pyspark import SparkContext

def get_host(url):
  try:
    return urlparse(url).hostname
  except:
    return None

def read_rdd(input_path):
  rdd = sc.textFile(input_path, use_unicode=False).cache()
  rdd = rdd \
    .filter(lambda r: len(r.split())==2) \
    .map(lambda r: (r.split("\t")[0], int(r.split("\t")[1])))
  return rdd

input_sitemap_spider_output = sys.argv[1]
input = sys.argv[2]
output = sys.argv[3]
sitemap_type = sys.argv[4]

sc = SparkContext()
rdd_0 = sc.textFile(input_sitemap_spider_output, use_unicode=False).cache()
rdd_0 = rdd_0 \
  .filter(lambda r: len(r.split("\t"))==8) \
  .filter(lambda r: r.split("\t")[-1].strip()==sitemap_type) \
  .map(lambda r: (r.split("\t")[3], r.split("\t")[1])) \
  .map(lambda p: (get_host(p[0]), get_host(p[1]))) \
  .filter(lambda p: p[0] != None and p[1] != None) \
  .distinct()

rdd = read_rdd(input)

rdd = rdd_0.join(rdd).map(lambda p: p[1]).reduceByKey(lambda acc,x: acc+x)

rdd.repartition(100).saveAsTextFile(output)
sc.stop()

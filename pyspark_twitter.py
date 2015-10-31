import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("twitter")
sc = SparkContext(conf = conf)
textFile = sc.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/yfliu/twitter_1000.json")

import json
from operator import add
filtered_rdd = textFile.map(lambda line: json.loads(line)).filter(lambda line: "text" in line)
split_rdd = filtered_rdd.flatMap(lambda line: line["text"].split(" "))
map_rdd = split_rdd.map(lambda item: (item, 1))
output = map_rdd.reduceByKey(add).collect()

for (word, count) in output:
	print ("%s: %i" %(word, count))

sc.stop()


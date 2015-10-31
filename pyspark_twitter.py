import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from pyspark import SparkConf, SparkContext

def analysis():
	conf = SparkConf().setAppName("twitter")
	sc = SparkContext(conf = conf)
	textFile = sc.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/yfliu/twitter_1000.json")

	import json
	from operator import add
	text_rdd = textFile.map(lambda line: json.loads(line)).filter(lambda line: "text" in line)
	filtered_rdd = text_rdd.filter(lambda line: line["lang"] == "en")
	split_rdd = filtered_rdd.flatMap(lambda line: line["text"].split(" "))
	map_rdd = split_rdd.map(lambda item: (item, 1))
	output = map_rdd.reduceByKey(add).takeOrdered(10, key=lambda x: -x[1])

	for (word, count) in output:
		print ("%s: %i" %(word, count))

		sc.stop()

if __name__ == '__main__':
	analysis()
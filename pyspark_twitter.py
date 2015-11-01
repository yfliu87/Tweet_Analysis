import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from pyspark import SparkConf, SparkContext
from stop_words import get_stop_words
import json

separator = [",",":","@"," ","!","#","$","%","*","...","(",")","~","","-"]
stopWords = get_stop_words('english') 

def readline(line):
	try:
		return json.loads(line)
	except:
		return json.dumps([])

def analysis():
	conf = SparkConf().setAppName("twitter")
	sc = SparkContext(conf = conf)
	textFile = sc.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/yfliu/twitter_10000.json")

	text_rdd = textFile.map(lambda line: readline(line)).filter(lambda line: "text" in line)
	filtered_rdd = text_rdd.filter(lambda line: line["lang"] == "en")
	split_rdd = filtered_rdd.flatMap(lambda line: line["text"].split(" "))
	cached_rdd = split_rdd.cache()
	calculate_popular_words(cached_rdd)

	sc.stop()


def calculate_popular_words(rdd):
	from operator import add
	no_separator_rdd = rdd.filter(lambda item: item not in separator)
	no_stop_word_rdd = no_separator_rdd.filter(lambda item: item not in stopWords)
	map_rdd = no_stop_word_rdd.map(lambda item: (item, 1))
	output = map_rdd.reduceByKey(add).takeOrdered(10, key=lambda x: -x[1])

	for (word, count) in output:
		print ("%s: %i" %(word, count))


if __name__ == '__main__':
	analysis()

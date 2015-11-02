import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from pyspark import SparkConf, SparkContext
from stop_words import get_stop_words
import json

separator = [",",":","@"," ","!","#","$","%","*","...","(",")","~","","-"]
stopWords = get_stop_words('english') 
negative_file = "/home/cloudera/datasets/negative-words.txt"
positive_file = "/home/cloudera/datasets/positive-words.txt"

def readline(line):
	try:
		return json.loads(line)
	except:
		return json.dumps([])

def analysis():
	conf = SparkConf().setAppName("twitter")
	sc = SparkContext(conf = conf)
	textFile = sc.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/yfliu/twitter_10000.json")

	english_rdd = textFile.map(lambda line: readline(line)).filter(lambda line: "lang" in line and line["lang"] == "en")
	text_rdd= english_rdd.filter(lambda line: "text" in line)
	cached_rdd = text_rdd.cache()
	calculate_popular_words(cached_rdd)
	sentiment_analysis(cached_rdd)

	sc.stop()


def calculate_popular_words(rdd):
	from operator import add
	split_rdd = rdd.flatMap(lambda line: line["text"].split(" "))
	no_separator_rdd = split_rdd.filter(lambda item: item not in separator)
	no_stop_word_rdd = no_separator_rdd.filter(lambda item: item not in stopWords)
	map_rdd = no_stop_word_rdd.map(lambda item: (item, 1))
	output = map_rdd.reduceByKey(add).takeOrdered(10, key=lambda x: -x[1])

	for (word, count) in output:
		print ("%s: %i" %(word, count))


def sentiment_analysis(rdd):
	positive_word_bag = read_words(positive_file)
	negative_word_bag = read_words(negative_file)
	tweet_sent_rdd = rdd.map(lambda item: (item['text'], get_sentiment(item['text'], positive_word_bag, negative_word_bag)))
	sent_tweet_rdd = tweet_sent_rdd.map(lambda item: swap(item))
	sentiment_tweet = sent_tweet_rdd.groupByKey().takeOrdered(10, key=lambda x: -x[0])

	for (sent, tweets) in sentiment_tweet:
		print "\nsentiment value: ", sent
		print "tweets count: ", len(tweets)
	

def read_words(word_file):
	reader = open(word_file, 'r')
	words = set()

	for word in reader:
		words.add(word.split("\n")[0])

	return words
	

def swap(item):
	return (item[1], item[0])


def get_sentiment(text, positive_word_bag, negative_word_bag):
	words = text.split(" ")
	
	sentiment = 0
	for word in words:
		if word in positive_word_bag:
			sentiment += 1 

		if word in negative_word_bag:
			sentiment -= 1
	
	return sentiment



if __name__ == '__main__':
	analysis()

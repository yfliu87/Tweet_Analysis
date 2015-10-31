from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("twitter")
sc = SparkContext(conf = conf)
textFile = sc.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/yfliu/twitter_1000.json")
print textFile.count()
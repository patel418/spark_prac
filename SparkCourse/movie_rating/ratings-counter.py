from pyspark import SparkConf, SparkContext
import collections
# set master node as local.

#Get Spark Configuration
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram") 
sc = SparkContext(conf = conf)

#textFile() breaks each entry in file line by line so each line has 1 entry in rdd
lines = sc.textFile("ml-100k/u.data")
#lines.map puts information into a new rdd (user id, movie id, rating, timestamp)
ratings = lines.map(lambda x: x.split()[2])
#countByValue() counts uniqueness of each occurrance. since countByValue() is an action, result is just a python object, not an rdd anymore.
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))

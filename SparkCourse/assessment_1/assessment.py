from pyspark import SparkConf, SparkContext

def parseLine(line):
    fields = line.split(",")
    custId = fields[0]
    spent = fields[2]
    return (int(custId), float(spent))

conf = SparkConf().setMaster("local").setAppName("Assessment1")
sc = SparkContext(conf = conf)

lines = sc.textFile("customer-orders.csv")
parsed = lines.map(parseLine)
custIdTotal = parsed.reduceByKey(lambda x, y: x + y)
result = custIdTotal.collect()

for result in result:
    #print("(" + r1 + ", " + r2 + ")")
    print(result)


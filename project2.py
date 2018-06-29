import sys
import json
from pyspark import SparkContext
#hashmap

##./bin/spark-submit --master local[8] --driver-memory 2G project2.py /Users/liufang/Documents/BitTiger-CS502-1802/data/ads_0502.txt
##spark-submit --master local[8] --driver-memory 2G /Users/liufang/Documents/BitTiger-CS502-1802/week7/project2.py /Users/liufang/Documents/BitTiger-CS502-1802/data/ads_0502.txt
def get_term(line):
    entry = line.split('_')
    return entry[1]

def get_terms_bidprice(line):
    entry = json.loads(line.strip())
    
    bid_price = entry['bidPrice']
    adid_terms = []
    #print entry['keyWords']
    #use hashmap to dedupe adid_term
    for term in entry['keyWords']:
        val = (term,float(bid_price))
        adid_terms.append(val)
    return adid_terms

def generate_json(items):
    return (items[1],items[0])

def generate_json2(items):
    return (items[1],items[0])

if __name__ == "__main__":
    adfile = sys.argv[1] #raw ads data
    #adfile="/Users/liufang/Documents/BitTiger-CS502-1802/data/ads_0502.txt"
    sc = SparkContext(appName="cost_avg")
    data=sc.textFile(adfile).flatMap(lambda line: get_terms_bidprice(line)).reduceByKey(lambda v1,v2: (v1+v2)/2).map(lambda pair:(pair[1],pair[0])).sortByKey(ascending=False).map(lambda pair:(pair[1],pair[0]))
    data.saveAsTextFile("/Users/liufang/Documents/BitTiger-CS502-1802/data8")
    #data.saveAsTextFile("/Users/liufang/Documents/BitTiger-CS502-1802/data6")
    sc.stop()
import sys
import json
from pyspark import SparkContext


def get_term(line):
    entry = line.split('_')
    return entry[1]

def get_terms_bidprice(line):
    entry = json.loads(line.strip())
    
    bid_price = entry['bidPrice']
    adid_terms = []
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
    sc = SparkContext(appName="cost_avg")
    data=sc.textFile(adfile).flatMap(lambda line: get_terms_bidprice(line)).reduceByKey(lambda v1,v2: (v1+v2)/2).map(lambda pair:(pair[1],pair[0])).sortByKey(ascending=False).map(lambda pair:(pair[1],pair[0]))
    data.saveAsTextFile("data8")
    sc.stop()
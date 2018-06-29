import sys
import json
from pyspark import SparkContext


def get_ad_pair(line):
    entry = json.loads(line.strip())
    return (entry['campaignId'],str(entry['bidPrice'])+'_'+str(entry['adId']))

def get_budget_pair(line):
    entry = json.loads(line.strip())
    #print entry
    return (entry['campaignId'],entry['budget'])

def get_bidprice(line):
    entry = line.split('_')
    return float(entry[0])

def get_adId(line):
    entry = line.split('_')
    return entry[1]


if __name__ == "__main__":
    adfile = sys.argv[1] #raw ads data
    budgetfile = sys.argv[2] #raw budgetfile data
    sc = SparkContext(appName="campaign_bidprice_avg")
    adlist=sc.textFile(adfile).map(get_ad_pair)
    bglist=sc.textFile(budgetfile).map(get_budget_pair)
    df = adlist.leftOuterJoin(bglist).distinct()
    df=df.map(lambda pair:(pair[1][1]/get_bidprice(pair[1][0]),get_adId(pair[1][0]))).sortBy(lambda pair:(pair[0],pair[1]),ascending=True).map(lambda pair:(pair[1],pair[0]))
    df.saveAsTextFile("data11")
    sc.stop()
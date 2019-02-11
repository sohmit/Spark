#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Oct 20 20:11:12 2018

@author: sohinimitra
Q4 using pySpark
"""

import sys
import operator
from pyspark import SparkContext, SparkConf

def splitBusinessData(line):
    return (line[0],(line[1],line[2]))

def splitReviewData(line):
    return (line[2],(float(line[3])))

def formatJoinData(line):
    return (line[1][1],(line[1][0],line[0]))

def finalOutput(line):
    rating = str(line[1][0])
    address = line[1][1][0]
    categories = line[1][1][1]
    businessID = line[0]
    return("{0}\t{1}\t{2}\t{3}".format(businessID,address,categories,rating))
    
def reverseKey(line):
    return (line[1],(line[0]))



if __name__ == "__main__":
    sc.stop()
    config = SparkConf().setAppName("Average Ratings").setMaster("local[2]")
    sc = SparkContext(conf = config)
    
    
    business = sc.textFile("business.csv")
    business = business.map(lambda x : x.split("::"))
    business = business.map(splitBusinessData)
   
    review = sc.textFile("review.csv")
    review = review.map(lambda x : x.split("::"))
    review = review.map(splitReviewData)
    review = review.mapValues(lambda x: (x, 1))
    review = review.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
    review = review.mapValues(lambda x: x[0]/x[1])
    review = review.map(reverseKey)
    review = review.sortByKey(ascending = False)
    review = review.take(10)
    review = sc.parallelize(review)
    review = review.map(reverseKey)
    print(review.take(10))
    joinData = review.join(business)
 
    output = joinData.map(finalOutput)
    print(output.take(20))
    #output.coalesce(1).saveAsTextFile("Output_Q4.txt")
    
    
   

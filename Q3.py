#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Oct 20 14:22:53 2018

@author: sohinimitra
Q3 Using pySpark

"""

import sys
from pyspark import SparkContext, SparkConf

def getUserID(line):
    return (line[0], (line[1]))

def getUserIDAndRating(line):
    return (line[2], (line[3], line[1]))

def getOutput(line):

    userid = line[1][1][1]
    rating = line[1][1][0]
  
    return("{0}\t{1}".format(userid,rating))
 

if __name__ == "__main__":
    sc.stop()
    config = SparkConf().setAppName("Stanford Ratings").setMaster("local[2]")
    sc = SparkContext(conf = config)
    
    business = sc.textFile("business.csv")
    business = business.map(lambda x : x.split("::"))
    business = business.filter(lambda x: "Stanford" in x[1])
    business = business.map(getUserID)
    review = sc.textFile("review.csv")
    review = review.map(lambda x : x.split("::"))
    review = review.map(getUserIDAndRating)
    joinData = business.join(review)
    output = joinData.map(getOutput)
  
    output.coalesce(1).saveAsTextFile("Output_Q3.txt")
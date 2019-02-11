#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 18 20:37:37 2018

@author: sohinimitra

"""

import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import *

def createMutualFriendsList(line):
    
  user = line[0].strip()
  friends = line[1]
  friendList = []
  if(user != ''):
      for f in friends:
          f = f.strip()
          if(f != ''):
              if(int(user)<int(f)):
                  listValue = user + "," + f , set(friends)
              if(int(f)<int(user)):
                  listValue = f + "," + user , set(friends)
              friendList.append(listValue)
  return(friendList)


def countFriends(line):
    return("{0}\t {1}".format(line[0],len(line[1])))

if __name__ == "__main__":
    sc.stop()
    config = SparkConf().setAppName("MutualFriends").setMaster("local[2]")
    sc = SparkContext(conf = config)
    spark = SparkSession(sc)
    lines = sc.textFile("soc-LiveJournal1Adj.txt")
    lines = lines.map(lambda x : x.split("\t"))
    lines = lines.filter(lambda x : len(x) == 2)
    lines = lines.map(lambda x : [x[0],x[1].split(",")])
    lines = lines.flatMap(createMutualFriendsList)
    lines = lines.reduceByKey(lambda x,y: x.intersection(y))
    lines = lines.map(countFriends)
 
    lines.coalesce(1).saveAsTextFile("Output_Q1.txt")



  

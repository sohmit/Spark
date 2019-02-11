#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 24 14:13:05 2018

@author: sohinimitra

Q3 - Collaborative Filetring 
"""

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating

def getData(line):
    return (int(line[0]), int(line[1]), int(line[2]))
    
conf = SparkConf().setMaster("local").setAppName("q3")
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

data = sc.textFile("/Users/sohinimitra/Documents/ratings.dat").map(lambda line: line.split("::"))
data = data.map(getData)

#.map(lambda (u_id, m_id, rating, timestamp): Rating(int(u_id), int(m_id), int(rating)))

splits = data.randomSplit([6,4],24)
train_data = splits[0]
test_data = splits[1]
rank =10
numIterations = 20

model = ALS.train(train_data, rank, numIterations)

test_label= test_data.map(lambda p: ((p[0],p[1]),p[2]))
test_data= test_data.map(lambda p: (p[0],p[1]))

predictions = model.predictAll(test_data).map(lambda r: ((r[0], r[1]), r[2])) 
combined_result = predictions.join(test_label)
MSE = combined_result.map(lambda r : (r[1][0] - r[1][1])**2).mean() 
print(str(MSE)) 



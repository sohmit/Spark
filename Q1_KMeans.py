#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 21 13:38:13 2018

@author: sohinimitra

Q1 - KMeans
"""


from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.ml.clustering import KMeans
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors



conf =SparkConf().setMaster("local").setAppName("q1")
sc =SparkContext(conf=conf)
spark= SparkSession(sc)

def getKeyValue(line):
    
    line = [int(ele) for ele in line]
    
    return( (line[0],line[1:]) )
    
    

#item_user_mat=sc.textFile("/Users/sohinimitra/Documents/itemusermat").map(lambda x: x.split(" ")).map(lambda x: [[x[0],x[1:]] for y in x])
item_user_mat=sc.textFile("/Users/sohinimitra/Documents/itemusermat").map(lambda x: x.split(" "))
item_user_mat = item_user_mat.map(getKeyValue)

ratings =item_user_mat.map(lambda x: x[0]).zipWithIndex().map(lambda x: (x[1],x[0]))


data =[(Vectors.dense(x[1]),) for x in item_user_mat.collect()]
item_user_mat_df =spark.createDataFrame(data,["features"])

kmeans = KMeans(k=10,seed=1)
model =kmeans.fit(item_user_mat_df)

transformed=model.transform(item_user_mat_df).select("features","prediction")
transformed_with_index =transformed.rdd.zipWithIndex()
rows =transformed_with_index.collect()
prediction_with_index =sc.parallelize(rows).map(lambda x: (x[1],x[0].prediction))

ratingsPrediction = ratings.join(prediction_with_index).map(lambda x: x[1])


movie=sc.textFile("/Users/sohinimitra/Documents/movies.dat").map(lambda x: x.split("::")).map(lambda x: ( int(x[0]),(x[1],x[2])))
moviePredictions =movie.join(ratingsPrediction).map( lambda x: ( ( x[1][1], ( x[0],x[1][0],x[1][1] )  ) ) )
result =moviePredictions.groupByKey().map(lambda x:list(x[1])[0:5]).flatMap(lambda x:x).collect()
sc.parallelize(result).saveAsTextFile("q1.txt")





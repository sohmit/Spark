#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 24 12:37:16 2018

@author: sohinimitra

Q2 - Decision Tree and Naive Bayes
"""


from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.classification import NaiveBayes
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors

conf = SparkConf().setMaster("local").setAppName("q2")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
glass=sc.textFile("/Users/sohinimitra/Documents/glass.data").map(lambda x: x.split(",")).map(lambda x:[float(y) for y in x])
feature_vector=glass.map(lambda x: (x[-1],Vectors.dense(x[1:-1])))

feature_vector = spark.createDataFrame(feature_vector.collect(),["label","features"])

splits= feature_vector.randomSplit([0.6,0.4],seed=24)
dt = DecisionTreeClassifier(maxDepth=10,labelCol="label")
model =dt.fit(splits[0])
test_label=splits[1].select("label").collect()
test_data=splits[1].select("features")
result= model.transform(test_data).select("prediction").collect()
sum_1=0
for i in range(len(result)):
    if result[i].asDict()['prediction']==test_label[i].asDict()['label']:
        sum_1 +=1
print("Decision Tree Classifier Accuracy", sum_1/float(len(result)))


nb=NaiveBayes(smoothing=1.0,modelType="multinomial")
model=nb.fit(splits[0])
result=model.transform(test_data).select("prediction").collect()
sum_1=0
for i in range(len(result)):
    if result[i].asDict()['prediction']==test_label[i].asDict()['label']:
        sum_1+=1
print("Naive Bayes Accuracy", sum_1/float(len(result)))
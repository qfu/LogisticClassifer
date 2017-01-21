from __future__ import print_function

import sys

from pyspark import SparkConf,SparkContext
from random import randint

from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel,LogisticRegressionWithSGD


def parsePoint(line):
    """
    Parse a line of text into an MLlib LabeledPoint object.
    """

    # For single line record single occurance of word
    linedict = {}
    for word in line.split(' '):
        linedict[word] = 1

    values = []

    # Build feature vector based on words frequencies
    for k,v in features.items():
        if linedict.get(k) == 1:
            values.append(v)
        else: 
            values.append(0) # if word does not appear append 0
 
    # Randomly give classificaiton result 
    classResult  = randint(0, 1)

    # Return RDD labelPoint data
    return LabeledPoint(classResult, values)




if __name__ == "__main__":


    #Setting up the standalone mode
    conf = SparkConf().setMaster("local").setAppName("LogisticClassifer")
    sc = SparkContext(conf = conf)  

    # Load in RDD and RDD persist for multiple operation
    lines = sc.textFile("/Users/fuyinlin/Desktop/CS_244A/python/random.txt").persist();

    # Produce the features hash map, features (not int RDD) bring back to driver
    # Transformatoin -> Action job
    features = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .countByKey()


    # [class, features ...]
    points = lines.map(parsePoint)
    # model = LogisticRegressionWithLBFGS.train(points)

    # Train with 1000 iterations see how classifer converages
    iterations = int(1000)
    model = LogisticRegressionWithSGD.train(points, iterations)

    #Print some weight and intercept, from logistic regression example
    print("Final weights: " + str(model.weights))
    print("Final intercept: " + str(model.intercept))
    sc.stop()
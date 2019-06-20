import os
os.environ["PYSPARK_PYTHON"]="/usr/bin/python3"

from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import StreamingLinearRegressionWithSGD,StreamingLinearAlgorithm
from pyspark.streaming import StreamingContext
from pyspark.mllib.regression import LabeledPoint,LinearRegressionWithSGD,LinearRegressionModel
from pyspark import SparkContext
from pyspark import SparkConf

def convertToFloat(lines):
    returnedLine = []    
    for x in lines:
        returnedLine.append(float(x))        
    return returnedLine

def convert(rdd):
    tmp = rdd.map(lambda line:line.encode("ascii", "ignore").strip().split()).map(convertToFloat)
    print(tmp)

#count = 0

def sql(records):
    from firebase import firebase

    url = 'https://cloud-hw-82029.firebaseio.com'
    fb = firebase.FirebaseApplication(url, None)
    count = fb.get('/ml', 'count')
    #for item in records.collect():
    for item in records.collect():
        fb.patch(url + '/ml/gt', {str(count):item[0]})
        fb.patch(url + '/ml/predict', {str(count):item[1]})
        count += 1
    fb.patch(url + '/ml', {'count':count})
    print(count)

def partition(rdd):
    partRDD = rdd.repartition(3)
    partRDD.foreachPartition(sql)
#import logging

conf = SparkConf()
conf.setMaster('local[2]')
conf.setAppName("ZZZ")
#logger = logging.getLogger('pyspark')
sc = SparkContext(conf = conf)
ssc = StreamingContext(sc,5)
mylog = []

def parse(lp):
    #label = float(lp[ lp.find('(')+1:lp.find(',')])
    #vec = Vectors.dense(lp[lp.find('[')+1:lp.find(',')].split(','))
    #return LabeledPoint(label,vec)
    values = [float(x) for x in lp.replace(',', ' ').split(' ')]
    #print(values[1:])
    return LabeledPoint(values[0], values[1:])

trainingData = ssc.textFileStream("/root/training").map(parse).cache()
testData = ssc.textFileStream("/root/testing").map(parse)

numFeatures = 3
#model = StreamingLinearRegressionWithSGD(StreamingLinearAlgorithm).setInitialWeights(Vectors.zeros(numFeatures))
model = StreamingLinearRegressionWithSGD(stepSize=0.001)
#model.algorithm.setIntercept(True)
model.setInitialWeights([0.0,0.0,0.0])

model.trainOn(trainingData)
#model.predictOnValues(testData.map(lambda lp:(lp.label,lp.feature))).saveAsTextFiles("file:///root/output.txt")

#mylog.append(model.predictOnValues(testData.map(lambda lp:(lp.label,lp.features))))

predict = model.predictOnValues(testData.map(lambda lp:(lp.label,lp.features)))
#predict = model.predictOn(testData.map(lambda lp:lp.features))

#testData.map(lambda lp:lp.features).foreachRDD(sql)

#predict = testData.map(lambda lp:(model.predictOnValues(lp.features)))
#predict = model.predictOnValues(testData.map(lambda lp:lp.features))

predict.pprint()
predict.foreachRDD(sql)
#model.trainOn(testData)

#tmp = predict.map(lambda line:line.encode("ascii", "ignore").strip().split()).map(convertToFloat)
#print(tmp)

#print(mylog)
ssc.start()
ssc.awaitTermination()

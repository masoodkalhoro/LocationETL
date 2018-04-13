# Import required pyspark libraries
import pyspark.sql as sq
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
import os

# spark application has been run on spark local if you want to run on cluser kindly set SparkContext 
sc = SparkContext("local", "ETL")
spark = SQLContext(sc)

########################################################################################################################
# Apache Spark application in pyspark that reads the data from csv file 
# 1. Cleanup (Spark Job)
# 2. Label (Spark Job)
# 3. Analysis (Spark Job)
# 4. Model
########################################################################################################################
# load csv data #
def load_csv(filePath,fileName):
    """ Load csv files """
    return spark.read.csv(os.path.join(filePath,fileName),header=True,inferSchema = True)
poiList= load_csv("data/","POIList.csv")
dataSet= load_csv("data/","DataSample.csv")

########################################################################################################################
# 1. Cleanup (Spark Job): 
########################################################################################################################

# Finding the suspicious IDs
def suspiciousIDs(data):
    """ Finding the suspicious IDs """
    return (data.groupBy([' TimeSt', 'Latitude', 'Longitude']).agg(collect_list("_ID").alias("_IDTemp")).where(size("_IDTemp") > 1)).select(explode("_IDTemp").alias("_ID"))
tempData = suspiciousIDs(dataSet)
# Removing the suspicous IDs
def removeSuspiciousIDs(tempData):
    return dataSet.join(tempData, dataSet._ID == tempData._ID, "left_anti").drop(tempData._ID)
data= removeSuspiciousIDs(tempData)

########################################################################################################################
# 2. Label (Spark Job)
########################################################################################################################
# assign each request in the `DataSample.csv` to one of those POI locations that has minimum distance to the request location
# Change poiList colums for easy to recognize
def changeColumNames(poi):
    return poi.select('POIID',col(" Latitude").alias("poiLatitude"), col("Longitude").alias("poiLongitude"))
poiList = changeColumNames(poiList)
# Join data with poiList
def join_poi_data(data):
    return data.crossJoin(poiList)
data= join_poi_data(data)
#defining a function for calculating distance between two points
def dis(lat1,lon1,lat2,lon2):
    R=6371 # Radius of the earth in km
    #Converts an angle measured in degrees to an approximately equivalent angle measured in radians
    lon1=toRadians(lon1)
    lat1=toRadians(lat1)
    lon2=toRadians(lon2)
    lat2=toRadians(lat2)
    # This uses the ‘haversine’ formula to calculate the great-circle distance between two points – that is, 
    # the shortest distance over the earth’s surface – giving an ‘as-the-crow-flies’ distance between the points 
    a=sin((lat1-lat2)/2)**2+cos(lat1)*cos(lat2)*sin((lon1-lon2)/2)**2
    c=2*atan2(sqrt(a),sqrt(1-a))
    distance=R*c
    return distance
# A new column created which contains distance bitween two points
def poiDistance(data):
    return data.withColumn("Poidistance", dis(data['Latitude'],data['Longitude'], data['poiLatitude'],data['poiLongitude']))
data= poiDistance(data)
# Find minimum distance poi
def minPoiDistance(data):
    return data.groupBy('_ID').min('Poidistance')
dataTemp= minPoiDistance(data)
# join minpoiDistance with data 
def joinMinPoiDistance(data):
    return data.join(dataTemp,(data['_ID'] == dataTemp['_ID']) & (data['Poidistance'] == dataTemp['min(Poidistance)'])).drop(dataTemp._ID)
data= joinMinPoiDistance(data)
# delet Poidistance column from data 
def deletPoidistance(data):
    return data.drop('Poidistance')
data= deletPoidistance(data)

########################################################################################################################
# 3. Analysis (Spark Job)
# With respect to each POI location, please calalculate the average and standard deviation of distance between the 
# POI location to each of its assigned records.
#Image to draw a circle which is centred at a POI location and includes all records assigned to the POI location. 
#Please find out the radius and density (i.e. record count/circle area) for each POI location.

########################################################################################################################

Q1=data.approxQuantile("min(Poidistance)", [0.35], 0.05)
Q3=data.approxQuantile("min(Poidistance)", [0.65], 0.05)
IQR = Q3[0] - Q1[0]
lowerRange = Q1[0] - 1.5*IQR
upperRange = Q3[0] + 1.5*IQR

#Average and Standard deviation outliers are included
def Avg_Std_Outliers_Included(data):
    analysis=data.groupBy('POIID').agg(avg("min(Poidistance)").alias('AVG(with outliers)'), stddev("min(Poidistance)").alias('STD(with outliers)'))
    return analysis
analysisA= Avg_Std_Outliers_Included(data)
#Average and Standard deviation outliers are not included
def Avg_Std_Outliers_Not_Included(data):
    analysis=data[(data['min(Poidistance)']>lowerRange) & (data['min(Poidistance)']<upperRange)].groupBy('POIID').agg(avg("min(Poidistance)").alias('AVG(without outliers)'), stddev("min(Poidistance)").alias('STD(without outliers)'))
    return analysis
analysisAB= Avg_Std_Outliers_Not_Included(data)
#Radius and Density outliers are included
def Rad_Den_Outliers_Included(data):
    analysis=data.groupBy('POIID').agg(max("min(Poidistance)").alias('Radius(with outliers)'), count("min(Poidistance)").alias('Count(with outliers)'))
    analysis=analysis.withColumn('density(with outliers)',analysis['Count(with outliers)']/(analysis['Radius(with outliers)']**2*np.pi))
    return analysis
analysisB=Rad_Den_Outliers_Included(data)
#Radius and Density outliers are not included
def Rad_Den_Outliers_Not_Included(data):
    analysis=data[(data['min(Poidistance)']>lowerRange) & (data['min(Poidistance)']<upperRange)].groupBy('POIID').agg(max("min(Poidistance)").alias('Radius(without outliers)'), count("min(Poidistance)").alias('Count(without outliers)'))
    analysis=analysis.withColumn('density(without outliers)',analysis['Count(without outliers)']/(analysis['Radius(without outliers)']**2*np.pi))
    return analysis
analysisBB=Rad_Den_Outliers_Not_Included(data)

########################################################################################################################
# 4. Model
######################################################################################################################## 
#MODEL: f(x)=(b-a)*(x-min)/(max-min)+a; b=10, a=-10
# Lets convert spark DataFram into panda DataFram for low level memory computation
analysisCA=analysisBB.toPandas() 
minn=np.min(analysisCA['density(without outliers)'])
maxx=np.max(analysisCA['density(without outliers)'])
analysisCA['density(mapped)']=(10-(-10))*(analysisCA['density(without outliers)']-minn)/(maxx-minn)+(-10)
analysisCA



########################################################################################################################
# 4. Model
########################################################################################################################
#Uncomment this section for seeing result on non-spark system
#import pandas as pd
#import matplotlib.pyplot as plt
#from sklearn.datasets import make_classification
#from sklearn.ensemble import ExtraTreesClassifier

#Lets convert spark DataFram into panda DataFram for low level memory computation
#bonus=data.toPandas()
# delete POI columns
#columns = ['_ID', 'poiLongitude', 'poiLatitude']
#bonus.drop(columns, inplace=True, axis=1)
#bonus.dtypes
# Take hour, day and month from TimeSt
#bonus['hour']=bonus[' TimeSt'].dt.hour
#bonus['day']=bonus[' TimeSt'].dt.day
#bonus['month']=bonus[' TimeSt'].dt.month

# Check the variation 
#bonus.describe()

#forest = ExtraTreesClassifier(n_estimators=50,random_state=0)
#y=bonus.iloc[:,6]
#X=bonus.iloc[:,0:9]
#
# delete the columns POIID and TimeST
#X.drop('POIID',inplace=True, axis=1)
#X.drop(' TimeSt',inplace=True, axis=1)

#X2=pd.get_dummies(X)
#forest.fit(X2, y)

#features = X2.columns
#importances = forest.feature_importances_
#indices = np.argsort(importances)
#imp=pd.DataFrame()
#imp['features']=features[indices]
#imp['Score']=importances[indices]
#imp.sort_values('Score', ascending=False)







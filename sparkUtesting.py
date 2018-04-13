from sparkETL import load_csv
from sparkETL import suspiciousIDs
from sparkETL import removeSuspiciousIDs
from sparkETL import changeColumNames
from sparkETL import join_poi_data
from sparkETL import dis
from sparkETL import poiDistance
from sparkETL import minPoiDistance
from sparkETL import joinMinPoiDistance
from sparkETL import deletPoidistance

# 1. Test file POIList.csv is available 
def test_load_csv():
 csvPOIList = load_csv("dataTest/","POIList.csv")
 csvDataSample = load_csv("dataTest/","DataSample.csv")

csvPOIList = load_csv("dataTest/","POIList.csv")
csvDataSample = load_csv("dataTest/","DataSample.csv")

# 2. Test file records load currectly 
def test_count_record(): 
 assert csvPOIList.count() == 4
 assert csvDataSample.count() == 12


# Test suspicious IDs found correctly
tempData = suspiciousIDs(csvDataSample)
def test_suspiciousIDs_record():
 assert tempData.count() == 4

# Test suspicious IDs found correctly
data1= removeSuspiciousIDs(tempData)
def test_removeSuspiciousIDs_record():
 assert data1.count() == 8

# Test poiList after changing colum names
poiList = changeColumNames(csvPOIList)
def test_poiList_changeColumName_record():
 assert poiList.count() == 4

# Test data record after joining each poi
data2= join_poi_data(data1)
def test_data_join_record():
 assert data2.count() == 32

# Test distance bitween two points record 
data3= data2.withColumn("Poidistance", dis(data2['Latitude'],data2['Longitude'], data2['poiLatitude'],data2['poiLongitude']))
def test_distance_record():
 assert data3.count() == 32

# Test poiDistance column record 
data4= poiDistance(data3)
def test_distance_record():
 assert data4.count() == 32

# Test minPoiDistance column record 
data5= minPoiDistance(data4)
def test_minPoidistance_record():
 assert data5.count() == 8

# Test joinMinPoiDistance column record 
data6= joinMinPoiDistance(data4)
def test_joinMinPoiDistance_record():
 assert data6.count() == 8

# Test deletPoidistance column record 
data7= deletPoidistance(data6)
def test_deletPoidistance_record():
 assert data7.count() == 8







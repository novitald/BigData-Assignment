#todo: visualisasi

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

conf = SparkConf().setMaster('local').setAppName('InternationalStudentsByCountry')
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

import pyspark_csv as pycsv
sc.addPyFile('pyspark_csv.py')

def extract_row(row):
   country = row[2]
   year = row[13]
   internationalStudents = 0
   if (row[9] != None and row[11] != None):
      numStudents = float(str(row[9]).replace(',',''))
      internationalPercentage = float(str(row[11])[:-1])
      internationalStudents = int(numStudents * internationalPercentage / 100.0)
   return ((year, country), internationalStudents)

plaintext_rdd = sc.textFile('file:///Users/Wik/Documents/Kuliah/BigData/Tugas-2/WorldRankUniversity-Mining/data/timesData.csv')
rdd = pycsv.csvToDataFrame(sqlContext, plaintext_rdd).rdd
mapped = rdd.map(extract_row)
reduced = mapped.reduceByKey(lambda a, b : a + b)
sorted = reduced.sortByKey()
result = sorted.collect()
for item in result:
   print str(item[0][0]) + ' - ' + str(item[0][1]) + ': ' + str(item[1])
   

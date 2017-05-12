from pyspark import SparkContext
from pyspark.sql import SQLContext,Row
import matplotlib.pyplot as plt
sc = SparkContext(appName="PySparkSQL")
sqlContext = SQLContext(sc)
lines =sc.textFile("/user/cloudera/data.txt")
parts=lines.map(lambda l: l.split(" "))
data=parts.map(lambda p: Row(month=p[0],ahigh=float(p[1]),alow=float(p[2]),daymax=float(p[3]),daymin=float(p[4]),precipt=float(p[5]),windavg=float(p[6]),humidity=float(p[7])))
schemaData=sqlContext.createDataFrame(data)
schemaData.registerTempTable("data")
rainyMonths=sqlContext.sql("SELECT month FROM  data WHERE precipt>7.0")
SummerMonths=sqlContext.sql("SELECT month FROM  data WHERE ahigh>80")
WinterMonths=sqlContext.sql("SELECT month FROM  data WHERE alow<30")
MostWindyMonth=sqlContext.sql("SELECT MAX(windavg) FROM data")
plotahigh=sqlContext.sql("SELECT ahigh FROM  data").map(lambda p:p.ahigh)
plotalow=sqlContext.sql("SELECT alow FROM  data").map(lambda p:p.alow)
plotprecipt=sqlContext.sql("SELECT precipt FROM data").map(lambda p: p.precipt)
plothumid=sqlContext.sql("SELECT humidity FROM  data").map(lambda p: p.humidity)
plotwindavg=sqlContext.sql("SELECT windavg FROM  data").map(lambda p: p.windavg)
rainyMnths = rainyMonths.map(lambda p:p.month)
SummerMnths = SummerMonths.map(lambda p:p.month)
WinterMnths = WinterMonths.map(lambda p:p.month)

str=""
str=str+"Rainy Months:::: \n"
for rmonths in rainyMnths.collect():
	str=str+rmonths+" "
str=str+"\n"
str=str+"Summer Months:::: \n"
for smonths in SummerMnths.collect():
	str=str+smonths+" "
str=str+"\n"
str=str+"Winter Months:::: \n"
for wmonths in WinterMnths.collect():
	str=str+wmonths+" "
print str
sc.parallelize([str]).saveAsTextFile("/user/cloudera/output/")
collahigh=[]
collalow=[]
collprecipt=[]
collwindaverage=[]
for plot in plotahigh.collect():
	collahigh.append(plot)
for plot in plotalow.collect():
	collalow.append(plot)
for plot in plotprecipt.collect():
	collprecipt.append(plot)
for plot in plotwindavg.collect():
	collwindaverage.append(plot)

plt.figure(1).suptitle('Average Temperature High/Low', fontsize=16,fontweight='bold')
plt.plot(collahigh)
plt.plot(collalow)
plt.ylabel('Average Temperatures')
plt.xlabel('Months')
plt.figure(2).suptitle('Preciptation', fontsize=16,fontweight='bold')
plt.xlabel('Months')
plt.ylabel('Precipitation')
plt.plot(collprecipt)
plt.figure(3).suptitle('WindAverage', fontsize=16,fontweight='bold')
plt.xlabel('Months')
plt.ylabel('WindAverage')
plt.plot(collwindaverage)
plt.show()

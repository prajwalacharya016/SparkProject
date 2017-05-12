from pyspark.sql import SQLContext
from pyspark import SparkContext
# sc is an existing SparkContext.
from pyspark.sql import SQLContext, Row
sc = SparkContext(appName="PythonStreamingHDFSWordCount")
sqlContext = SQLContext(sc)

# Load a text file and convert each line to a Row.
lines = sc.textFile("/user/cloudera/people.txt")
parts = lines.map(lambda l: l.split(" "))
people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))

# Infer the schema, and register the DataFrame as a table.
schemaPeople = sqlContext.createDataFrame(people)
schemaPeople.registerTempTable("people")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

# The results of SQL queries are RDDs and support all the normal RDD operations.
teenNames = teenagers.map(lambda p: "Name: " + p.name)
for teenName in teenNames.collect():
  print(teenName)

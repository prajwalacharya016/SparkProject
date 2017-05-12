import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
def checkoxlev(x):
	if float(x)>59:
		return "Oxygen Mask Status: No emergency oxy level"
	else:
		return "Oxygen Mask Status: We will like everyone to please have their oxygen mask on"

def checkheight(x):
	if float(x)>5000:
		return "Seat belt Notification: No Seat Belt compulsory"
	else:
		return "Seat belt Notification: We ask everyone to please have their seatbelt on"

if __name__ == "__main__":
	sc = SparkContext(appName="PythonStreamingHDFSWordCount")
	ssc = StreamingContext(sc, 10)
	myFile = ssc.textFileStream("/user/cloudera/input/")
	words = myFile.map(lambda line: line.split(" "))
	distance=words.map(lambda mymap:mymap[0])
	dFInit=words.map(lambda mymap:mymap[1])
	distCovered=words.map(lambda mymap:"Distance Covered: "+mymap[1])
	height=words.map(lambda mymap:mymap[2])
	myheight=words.map(lambda mymap:"Current height: "+mymap[2])
	oxlevel=words.map(lambda mymap:mymap[3])
	airplaneweight=words.map(lambda mymap:"Airplane Weight: "+mymap[4])
	velocity=distance.map(lambda dist:"Current Velocity: "+str(float(dist)*360))
	distancetoDestination=dFInit.map(lambda dist:"Distance remaining: "+str(12000-float(dist)))
	oxyLevel = oxlevel.map(lambda lev: checkoxlev(lev))
	seatBeltNotific = height.map(lambda ht: checkheight(ht))
	firstunion=ssc.union(velocity,distancetoDestination)
	secondunion=ssc.union(firstunion,distCovered)
	thirdunion=ssc.union(secondunion,myheight)
	fourthunion=ssc.union(thirdunion,airplaneweight)
	fifthunion=ssc.union(fourthunion,oxyLevel)
	unionstream=ssc.union(fifthunion,seatBeltNotific)
	unionstream.pprint()
	ssc.start()
	ssc.awaitTermination()

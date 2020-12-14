// Databricks notebook source
//  /FileStore/tables/nasa_august.tsv
//  /FileStore/tables/Property_data.csv
//  /FileStore/tables/nasa_july.csv
// /FileStore/tables/FriendsData.csv

// COMMAND ----------

val data =sc.textFile("/FileStore/tables/nasa_august.tsv")

// COMMAND ----------

val datajul =sc.textFile("/FileStore/tables/nasa_july.tsv")

// COMMAND ----------

val temp1 = data.map( x=> x.split("\t")(0))

temp1.collect()

// COMMAND ----------

temp1.count()

// COMMAND ----------

val temp2 = datajul.map( x=> x.split("\t")(0))

temp2.collect()

// COMMAND ----------

temp2.count()

// COMMAND ----------

val temp3 = temp1.filter(line => line != "host")

temp3.count()

// COMMAND ----------

val temp4 = temp2.filter(line => line != "host")

temp4.count()

// COMMAND ----------

val temp5 = temp3.intersection(temp4)

temp5.count()

// COMMAND ----------

val data = sc.textFile("/FileStore/tables/numbers.csv")

// COMMAND ----------

def Prime(i: Int):Boolean ={
  if(i<=1)
    false
  else if(i==2)
    true
  else 
    !(2 to (i-1)).exists( x => i%x == 0)
}

// COMMAND ----------

val head1 = data.first()
val temp7 = data.filter(line => line != head)

// COMMAND ----------

val temp8 = temp7.map(x-> x.toInt)
val out = temp8.filter(Prime)

// COMMAND ----------

val listdata = List("a 21", "b 22", "c 23", "d 24")

// COMMAND ----------

val rdd= sc.parallelize(listdata)
rdd.collect()

// COMMAND ----------

val newrdd = rdd.map(x=> (x.split(" ")(0),x.split(" ")(1).toInt))
newrdd.collect()

// COMMAND ----------

val newrdd2 = newrdd.mapValues(x=> x+10)
newrdd2.collect()

// COMMAND ----------

val data3 =sc.textFile("/FileStore/tables/airports.text")

// COMMAND ----------

val newrdd3 = data3.map(x=> (x.split(",")(1), x.split(",")(11).toLowerCase))
newrdd3.take(2)

// COMMAND ----------

val data4 = sc.textFile("/FileStore/tables/Property_data.csv")

// COMMAND ----------

data4.take(4)

// COMMAND ----------

// trying to clear data first
val rmvhead = data4.filter( line => !line.contains("Price"))

rmvhead.take(10)

// COMMAND ----------

val newrdd5 = rmvhead.map(x=> (x.split(",")(3).toInt,(1, x.split(",")(2).toDouble)))

newrdd5.take(4)

// COMMAND ----------

//trying to store data as key corresponds to a tuple whihc has frequency and sum

val redrdd =newrdd5.reduceByKey((x,y) => (x._1+y._1,x._2+y._2))

redrdd.take(6)

// COMMAND ----------

val outrdd = redrdd.mapValues( data => data._2 / data._1)
outrdd.collect()

// COMMAND ----------

for((a,b) <- outrdd.collect() ) println(a+" : "+b)

// COMMAND ----------

val data5 = sc.textFile("/FileStore/tables/FriendsData.csv")
data5.take(8)

// COMMAND ----------

val rmvhead = data5.filter( line => !line.contains("Id"))
rmvhead.take(10)

// COMMAND ----------

val newrdd6 = rmvhead.map(x=> (x.split(",")(2).toInt,(1, x.split(",")(3).toInt)))
newrdd6.take(4)

// COMMAND ----------

val redrdd2 =newrdd6.reduceByKey((x,y) => (x._1+y._1,x._2+y._2))
redrdd2.take(6)

// COMMAND ----------

val outrdd2 = redrdd2.mapValues( data => (data._2 / data._1).toInt)
outrdd2.collect()

// COMMAND ----------

val newrdd7 = rmvhead.map(x=> (x.split(",")(2).toInt,x.split(",")(3).toInt))
newrdd7.take(4)

// COMMAND ----------

def check(i: Int, j:Int):Int={
  if(i>j)
    i
  else 
    j
}

// COMMAND ----------

newrdd7.reduceByKey( check).collect()

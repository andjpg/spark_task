// Databricks notebook source
// /FileStore/tables/ratings.csv

// COMMAND ----------

// creating rdd from external dataset

val data =sc.textFile("/FileStore/tables/ratings.csv")

// COMMAND ----------

data.collect()

// COMMAND ----------

// the first step will be like trying to make some sense out of it
// i want to find one part rating

// COMMAND ----------

val ratingdt = data.map( x=> x.split(",")(2))
ratingdt.collect()

// COMMAND ----------

ratingdt.countByValue()

// COMMAND ----------



// COMMAND ----------

val dataair =sc.textFile("/FileStore/tables/airports.text")

dataair.collect()

// COMMAND ----------

def splitInput(line:String) = {
  
  val dataSplit = line.split(",")
  
  val airportId = dataSplit(3)
  
  val cityName = dataSplit(6)
  
  (airportId == "\"Iceland\"" || cityName .toDouble > 40)
}

// COMMAND ----------

def tasktwo(line:String) = {
  
  val dataSplit = line.split(",")
  
  val airportId = dataSplit(9)

  
  (airportId.toInt % 2 == 0)
}

// COMMAND ----------

val temp = dataair.filter(splitInput)

temp.take(5)

// COMMAND ----------

val temp2 = dataair.filter(tasktwo).map(x => x.split(",")(11))
temp2.collect()

// COMMAND ----------

  val tempdata = dataair.filter( x => x.split(",")(3)  == "\"United States\"")

// COMMAND ----------

tempdata.count()

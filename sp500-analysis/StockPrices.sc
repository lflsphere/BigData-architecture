import org.apache.spark.sql.types.{StructField, StructType, TimestampType, StringType, DoubleType}
import java.sql.Timestamp


val df = spark.read.option("header",true).csv("sp500_stock_price.csv")
df.show()
df.printSchema
// We redefine the schema
val mySchema = StructType(Array(
    StructField("Date", TimestampType, true),
    StructField("Open", DoubleType, true),
    StructField("High", DoubleType, true),
    StructField("Low", DoubleType, true),
    StructField("Close", DoubleType, true),
    StructField("Volume", DoubleType, true),
    StructField("Dividends", DoubleType, true),
    StructField("Stock Splits", DoubleType, true),
    StructField("Symbol", StringType, true),
    StructField("Name", StringType, true),
    StructField("Sector", StringType, true),
    StructField("Adj Close", StringType, false)
))
val df = spark.read.option("header",true).schema(mySchema).csv("data/sp500_stock_price.csv")
df.describe().show()
val newDF = df.drop("Adj Close")


// # Companies, # Sectors, # Companies per sector
newDF.map(x => (x.getAs[String]("Sector"))).distinct.count()
//SQL Style
newDF.selectExpr("Name").distinct.count  // Equivalent here
//SQL Style
newDF.selectExpr("Sector", "Name").distinct.groupBy("Sector").count().show()
//MapReduce Style With RDD
newDF.map(x => ((x.getAs[String]("Sector"), x.getAs[String]("Name")), 1)).rdd.groupByKey.map(x => (x._1._1, x._1._2)).countByKey
  

// Years with the lowest and highest total volume
newDF.map(x => (x.getAs[Timestamp]("Date").getYear + 1900, x.getAs[Double]("Volume"))).rdd.reduceByKey(_ + _).reduce((x, y) => if (x._2 < y._2) y else x)
newDF.map(x => (x.getAs[Timestamp]("Date").getYear + 1900, x.getAs[Double]("Volume"))).rdd.reduceByKey(_ + _).reduce((x, y) => if (x._2 > y._2) y else x)
// Using caching makes the second computation much faster.
val tmp = newDF.map(x => (x.getAs[Timestamp]("Date").getYear + 1900, x.getAs[Double]("Volume"))).rdd.reduceByKey(_ + _).cache()
tmp.reduce((x, y) => if (x._2 > y._2) y else x)
tmp.reduce((x, y) => if (x._2 < y._2) y else x)


// Year in which the stock market varied the most
val lowDate = newDF.map(x => (x.getAs[Timestamp]("Date").getYear + 1900, x.getAs[Double]("Low"))).rdd.groupByKey.map(x => (x._1, x._2.min))
val highDate = newDF.map(x => (x.getAs[Timestamp]("Date").getYear + 1900, x.getAs[Double]("High"))).rdd.groupByKey.map(x => (x._1, x._2.max))
lowDate.union(highDate).groupByKey.map(x => (x._1._1, x._2.max - x._2.min)).groupByKey().map(x => (x._1, x._2.sum / x._2.size)).reduce((x, y) => if (x._2 < y._2) y else x)


// Company that has the highest VolumeExchanged/SharesOutstanding ratio for each year
val mySchema = StructType(Array(
    StructField("Symbol", StringType, true),
    StructField("shareOutstanding", DoubleType, true)))
val so = spark.read.option("header",true).schema(mySchema).csv("data/sharesOutstanding.csv")
val soMap = so.collect.map(x => (x(0), x(1))).toMap.asInstanceOf[Map[String,Double]]
  
val lookup = sc.broadcast(soMap)
newDF.map(x => ((x.getAs[Timestamp]("Date").getYear + 1900, x.getAs[String]("Symbol")), x.getAs[Double]("Volume"))).rdd.reduceByKey(_ + _).map(x => (x._1._1, (x._1._2, x._2 / lookup.value.getOrElse[Double](x._1._2, -1)))).reduceByKey((x, y) => if (x._2 < y._2) y else x).map(x => (x._1, x._2._1)).sortByKey().collect()
  

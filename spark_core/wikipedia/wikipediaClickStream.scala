import scala.collection.mutable._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive
import org.apache.spark.sql.hive._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SaveMode._
import scala.util.control.Breaks._
val hiveContext=sqlContext.asInstanceOf[HiveContext]


val clickStream201502=hiveContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t").option("mode", "PERMISSIVE").option("inferSchema", "true").load("hdfs://209.126.98.253:8020/2015_02_clickstream.tsv")
clickStream201502.cache
clickStream201502.count
clickStream201502.show(truncate=false)


val clickStreamSelect201502=clickStream201502.select($"n", $"prev_title", $"curr_title", $"type")

// top 10 articles requested from wikipedia

val topTenArticles201502=clickStreamSelect201502.groupBy("curr_title").sum().select($"curr_title", $"sum(n)").orderBy($"sum(n)".desc).limit(10)

//  top referers to wikipedia

val topReferers201502=clickStreamSelect201502.groupBy("prev_title").sum().orderBy($"sum(n)".desc)

// top 5 trending articles

val topFiveArticles201502=clickStreamSelect201502.filter("prev_title = 'other-twitter'").groupBy("curr_title").sum().orderBy($"sum(n)".desc)

//  most requested missing pages

val missingPages201502=clickStreamSelect201502.filter("type = 'redlink'").groupBy("curr_title").sum().orderBy($"sum(n)".desc)

// traffic inflow vs outflow look like for the most requested pages

val pageviewsperarticle201502=clickStreamSelect201502.groupBy("curr_title").sum().withColumnRenamed("sum(n)", "in_count").cache()

val linksclickedperarticle201502=clickStreamSelect201502.groupBy("prev_title").sum().withColumnRenamed("sum(n)", "out_count").cache()

val traffic201502=pageviewsperarticle201502.join(linksclickedperarticle201502, ($"curr_title" === $"prev_title")).orderBy($"in_count".desc)

val ratio201502=traffic201502.withColumn("ratio", $"out_count"/$"in_count").cache()

ratio201502.select($"curr_title",$"in_count",$"out_count",$"ratio").show()




val clickStream201602=hiveContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t").option("mode", "PERMISSIVE").option("inferSchema", "true").load("hdfs://209.126.98.253:8020/2016_02_clickstream.tsv")
clickStream201602.cache
clickStream201602.count
clickStream201602.show(truncate=false)

val clickStreamSelect201602=clickStream201602.select($"n", $"prev", $"curr", $"type")

// top 10 articles requested from wikipedia

val topTenArticles201602=clickStreamSelect201602.groupBy("curr").sum().select($"curr", $"sum(n)").orderBy($"sum(n)".desc).limit(10).show()

//  top referers to wikipedia

val topReferers201602=clickStreamSelect201602.groupBy("prev").sum().orderBy($"sum(n)".desc).show()

// top 5 trending articles

val topFiveArticles201602=clickStreamSelect201602.filter("prev = 'other-twitter'").groupBy("curr").sum().orderBy($"sum(n)".desc).show()

//  most requested missing pages

val missingPages201602=clickStreamSelect201602.filter("type = 'redlink'").groupBy("curr").sum().orderBy($"sum(n)".desc).show()

// traffic inflow vs outflow look like for the most requested pages

val pageviewsperarticle201602=clickStreamSelect201602.groupBy("curr").sum().withColumnRenamed("sum(n)", "in_count").cache()

val linksclickedperarticle201602=clickStreamSelect201602.groupBy("prev").sum().withColumnRenamed("sum(n)", "out_count").cache()

val traffic201602=pageviewsperarticle201602.join(linksclickedperarticle201602, ($"curr" === $"prev")).orderBy($"in_count".desc)

val ratio201602=traffic201602.withColumn("ratio", $"out_count"/$"in_count").cache()

ratio201602.select($"curr",$"in_count",$"out_count",$"ratio").show()



val clickStream201603=hiveContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t").option("mode", "PERMISSIVE").option("inferSchema", "true").load("hdfs://209.126.98.253:8020/2016_03_clickstream.tsv")
clickStream201603.cache
clickStream201603.count
clickStream201603.show(truncate=false)

val clickStreamSelect201603=clickStream201603.select($"n", $"prev", $"curr", $"type")
// top 10 articles requested from wikipedia

val topTenArticles201603=clickStreamSelect201603.groupBy("curr").sum().select($"curr", $"sum(n)").orderBy($"sum(n)".desc).limit(10).show()

//  top referers to wikipedia

val topReferers201603=clickStreamSelect201603.groupBy("prev").sum().orderBy($"sum(n)".desc).show()

// top 5 trending articles

val topFiveArticles201603=clickStreamSelect201603.filter("prev = 'other-twitter'").groupBy("curr").sum().orderBy($"sum(n)".desc).show()

//  most requested missing pages

val missingPages201603=clickStreamSelect201603.filter("type = 'redlink'").groupBy("curr").sum().orderBy($"sum(n)".desc).show()

// traffic inflow vs outflow look like for the most requested pages
val pageviewsperarticle201603=clickStreamSelect201603.groupBy("curr").sum().withColumnRenamed("sum(n)", "in_count").cache()

val linksclickedperarticle201603=clickStreamSelect201603.groupBy("prev").sum().withColumnRenamed("sum(n)", "out_count").cache()

val traffic201603=pageviewsperarticle201603.join(linksclickedperarticle201603, ($"curr" === $"prev")).orderBy($"in_count".desc)

val ratio201603=traffic201603.withColumn("ratio", $"out_count"/$"in_count").cache()

ratio201603.select($"curr",$"in_count",$"out_count",$"ratio").show()

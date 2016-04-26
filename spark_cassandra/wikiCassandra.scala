import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive
import org.apache.spark.sql.hive._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.cassandra.CassandraSQLContext
val hiveContext=sqlContext.asInstanceOf[HiveContext]
val cc = new CassandraSQLContext(sc)

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


//writing data to cassandra

ratio201502.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "wiki201502", "keyspace" -> "wikiclickstream").save()


// reading data from cassandra

cassandraTable=sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "wiki201502", "keyspace" -> "wikiclickstream").load()

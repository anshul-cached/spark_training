import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.streaming._
import org.apache.spark.sql._ 
import org.apache.spark.streaming._
import org.apache.spark.SparkConf


  	
  	
  	val StreamingServerHost = "52.89.53.194"
  	val StreamingServerPort= 9002
  	val BatchInterval= Seconds(15)
  	val ssc = new StreamingContext(sc, BatchInterval)
  	println($"Connecting to $StreamingServerHost, port $StreamingServerPort")
  	val baseDS=ssc.socketTextStream(StreamingServerHost, StreamingServerPort)
  	
  	baseDS.foreachRDD { rdd => 
  	val sqlContext =SQLContext.getOrCreate(SparkContext.getOrCreate())
  	if(!rdd.isEmpty) {
  	val df = sqlContext.jsonRDD(rdd)
  	val df1=df.groupBy("user").count().withColumnRenamed("count", "user_count").cache()
  	val df2=df.groupBy("pageUrl").count().withColumnRenamed("count", "pageUrl_count").cache()
  	val df3=df2.join(df1, ($"user" === $"pageUrl")).orderBy($"pageUrl_count".desc).show()
  	}
  	}
  	ssc.start() 
  	ssc.awaitTermination()
  	}
  	}
  	

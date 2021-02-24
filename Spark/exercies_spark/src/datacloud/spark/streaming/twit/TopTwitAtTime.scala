package datacloud.spark.streaming.twit
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
object TopTwitAtTime {
	def main(args: Array[String])  {

		Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
		val spark = new SparkConf().setAppName("TopTwitAtTime").setMaster("local[*]")
		val sparkStremContext = new StreamingContext(spark, Seconds(1))
		sparkStremContext.checkpoint("/tmp/tmp")
		val stream = sparkStremContext.socketTextStream("localhost" , 4242 )

		val p = "#twit[0-9]+".r

				val l =stream.map(_.split(" "))
				.map(line => line.filter(f => p.findAllIn(f).hasNext))
				.flatMap(y => y).map(word =>(word,1)).reduceByKey(_+ _)
				.transform(rdd=>rdd.sortBy(_._2, false))
				.print()
				sparkStremContext.start()
				sparkStremContext.awaitTermination()
	}

}
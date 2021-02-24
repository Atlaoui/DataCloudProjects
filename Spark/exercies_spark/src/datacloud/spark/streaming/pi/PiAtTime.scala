package datacloud.spark.streaming.pi

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming
import org.apache.spark.storage.StorageLevel

object PiAtTime {
	def main(args: Array[String])  {
		Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
		val spark = new SparkConf().setAppName("PiAtTime").setMaster("local[*]")
		val sparkStremContext = new StreamingContext(spark, Seconds(10))
		sparkStremContext.checkpoint(",")
		val stream = sparkStremContext.socketTextStream("localhost" , 4242 )

		stream.map(_.split(" ")).map(elem => (elem(0).toDouble , elem(1).toDouble))
		.map(x =>(if(x._1*x._1 + x._2*x._2 <1 ) 1 else 0,1.toDouble)).reduceByKey(_+_)
		.map(elem => (elem._2,elem._2)).reduce((e1,e2) => (e1._2,e2._2)).map(x=>("Pi is roughly " + (4*x._2)/((x._1)+(x._2))))
		.print()

		sparkStremContext.start()
		sparkStremContext.awaitTermination()
	}
}
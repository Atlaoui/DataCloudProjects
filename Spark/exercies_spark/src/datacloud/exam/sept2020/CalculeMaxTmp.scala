package datacloud.exam.sept2020

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming
import org.apache.spark.storage.StorageLevel

object CalculeMaxTmp {
	def updateFunction(newValues: Seq[Float], runningVal: Option[Float]): Option[Float] = {
			runningVal match {
			case None => Some(newValues.max)
			case Some(value) => {val max = newValues.max  
			val  maxValue  = if( max > value) max else value 
			Some(maxValue)}
			}
	}

	def main(args: Array[String])  {
		Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
		val conf = new SparkConf().setAppName("Streaming").setMaster("local[*]")
		val sc = new SparkContext(conf)
		val ssc = new StreamingContext(sc, Seconds(10))
		val loc_temp_stream = ssc.socketTextStream("localhost", 4242).map(_.split(" "))
		                                     .map(a=> (a(0),a(1).toFloat))

		loc_temp_stream.updateStateByKey(updateFunction).print()

		ssc.checkpoint("/tmp/stream")
		ssc.start()
		ssc.awaitTermination()
	}
}
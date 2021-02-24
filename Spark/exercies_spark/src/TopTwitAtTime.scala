


import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
///home/adrien/SAR2_Assignments/DataCloud/TME/Spark/exercies_spark/src
object TopTwitAtTime extends App {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("toptwit").setMaster("local[*]")
    val ssc = new StreamingContext(new SparkContext(conf), Seconds(1))
    val lines = ssc.socketTextStream("localhost", 4242)
    ssc.checkpoint("tmpTwit")
    val Y = 30  
		val X = 10
    val split = lines.flatMap(_.split(" ").filter(y =>y.contains("#")))
    val s = split.map(x => (x,1))
    val window = s.reduceByKeyAndWindow((x:Int, y:Int) => x + y,_+_, Seconds(Y),Seconds(X))
    val cumul = window.transform(rdd => rdd.sortBy(elem => (elem._2, false)))
    cumul.print()
   
    ssc.start ()
    ssc.awaitTermination ()
}
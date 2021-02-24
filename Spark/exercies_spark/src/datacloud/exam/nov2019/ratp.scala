package datacloud.exam.nov2019
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
object ratp {
	//data : JJ_MM_AAAA_hh_mm_ss 1273453673 Place_Monge

	def nbUsersByStationAndByhour  (file:String, spark: SparkContext) {
		val line = spark.textFile(file);
		val pattern = "[0-9]*".r
				val l = line.map(l => l.split(" ")).filter(f => pattern.findAllIn(f(1)).hasNext).map(f => (f(2),(f(1),f(0).split("_")(3) ) )).groupByKey() 
				
				
	}
	def pageRank(file:String, out:String, iters: Int, spark: SparkContext)={
			val lines = spark.textFile(file)
					val links = lines.map(_.split(" "))
					.map(a=> (a(0),a(1)))
					.distinct()
					.groupByKey()
					links.cache()
					var ranks = links.mapValues(v => 1.0)
					for (i <- 1 to iters) {
						val join = links.join(ranks)
								val values = join.map(_._2)
								val tmp = values.flatMap(
										urls_rank => urls_rank._1.map(
												url => (url, urls_rank._2 / urls_rank._1.size)
												)
										)
								ranks = tmp.reduceByKey(_ + _)
					}
			ranks.saveAsTextFile(out)
	}
}
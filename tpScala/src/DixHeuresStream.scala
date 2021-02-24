import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.streaming._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.io._
import org.apache.hadoop.hbase.mapreduce._
import org.apache.hadoop.hbase.util._
import hbase.SparkHbase._
import collection.JavaConverters._

object DixHeuresStream {
	val conf = new SparkConf().setAppName("Stream Listening")
			val sc = new SparkContext(conf)
			val ssc = new StreamingContext(sc, Minutes(5))

			val stream1 = ssc.socketTextStream("listenstat.dixheures.com",532)

			val stream2 = stream1.map(s => s.split(" ")).map(a => (a(0),a(1),a(3).toInt - a(2).toInt))

			val stream3 = stream2.map(t => new Increment(Bytes.toBytes(t._2)).addColumn(Bytes.toBytes("cf"), Bytes.toBytes(t._1), t._3 ))

			stream3.foreachRDD(rdd => rdd.saveAsHbaseTable("ListeningStat","zoo1:zoo2:zoo3"));

	ssc.start()
	ssc.awaitTermination()


	def trackRanking():RDD[(String,Long)]={
			val table_listening = sc.hbaseTableRDD("ListeningStat", new Scan(), "zoo1;zoo2;zoo3")

			val rdd_idtrack_cumullistening = table_listening.map(c => (Bytes.toString(c._1.get), c._2.getFamilyMap(Bytes.toBytes("cf")).values()
            .asScala.map(Bytes.toLong(_)).reduce(_+_)))
		    
            
            val rdd_idtrack_cumullistening_sorted= rdd_idtrack_cumullistening.sortBy(_._2, false)

					val rdd_idtrack_titre = DixHeurs.loadTracks().map(t=> (t.idtrack,t.title));
			return rdd_idtrack_titre.join(rdd_idtrack_cumullistening_sorted).map(_._2)
	}
}
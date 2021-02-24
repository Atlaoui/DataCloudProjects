
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.io._
import org.apache.hadoop.hbase.mapreduce._
import org.apache.hadoop.hbase.util._
import org.apache.spark._
import org.apache.spark.rdd._
import hbase.SparkHbase.RDDHbase
import hbase.SparkHbase.MySparkContext
import org.apache.hadoop.hbase.client.Scan
import org.apache.log4j.Logger
import org.apache.log4j.Level
object CopyFromHbaseTohbase extends App {
	//https://gist.github.com/ishassan/c4d5770f4163e13a3e5a9b072e18ce7d
	Logger.getLogger("org.apache.spark").setLevel(Level.OFF)	
	val conf = new SparkConf().setAppName ("Spark␣on␣hbase").setMaster("local[*]")
	val sc = new SparkContext(conf)
	val scH =	 new MySparkContext(sc)
	
	//conf.set(TableInputFormat.INPUT_TABLE, "categoriecopie")
	
	val scans = new Scan()
	val rdd = scH.hbaseTableRDD("categorie", scans)
	
	val toPrint = rdd.map(f => (toString(f._2.listCells.get(0)),toString(f._2.listCells.get(1))))
	  
	val copy = rdd.map(f =>  new Put(f._1.get).add(f._2.listCells.get(0)).add(f._2.listCells.get(1)) )

	val c = new RDDHbase(copy)
	
	c.saveAsHbaseTable("categoriecopie")
	
  //toPrint.collect().foreach(f => println(f._1 +" " +f._2 ))
  
	def toString( a: Cell ) : String = {
			return  Bytes.toString(CellUtil.cloneValue(a))
	}
	
	sc.stop ()
}



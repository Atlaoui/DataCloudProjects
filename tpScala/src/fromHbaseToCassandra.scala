
import org.apache.spark._
import com.datastax.spark.connector._
import org.apache.spark.sql._
import scala.collection.Seq
import scala.reflect.runtime.universe
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.io._
import org.apache.hadoop.hbase.mapreduce._
import org.apache.hadoop.hbase.util._
import hbase.SparkHbase.RDDHbase
import hbase.SparkHbase.MySparkContext
import com.datastax.spark.connector.cql.{ColumnDef, RegularColumn, TableDef}
import com.datastax.spark.connector.types._


case class row(idcat: String, nameCat: String)


object fromHbaseToCassandra extends App{
  	//https://gist.github.com/ishassan/c4d5770f4163e13a3e5a9b072e18ce7d
	Logger.getLogger("org.apache.spark").setLevel(Level.OFF)	
	val spC = new SparkConf()
	val confHbase = spC.setAppName ("Spark␣on␣hbase").setMaster("local[*]")
	val sc = new SparkContext(confHbase)
	
	val scH =	 new MySparkContext(sc)
	
	val scans = new Scan()
	val rdd = scH.hbaseTableRDD("categorie", scans)
	val rdd23 = rdd.map(f => (toString(f._2.listCells.get(0)),toString(f._2.listCells.get(1))))
	              .map(f => new row(f._1,f._2))
	
	 val rddPrint = rdd.map(f => Bytes.toString(f._2.getValue("defaultcf".getBytes, "idcat".getBytes)))
	 
	 val rdd_to_put = rddPrint.map(f => (f,1)) 
	 
	 val to_put_in = rdd_to_put.map(f => new Put(f._1.getBytes).addColumn("defaultcf".getBytes, "rank".getBytes,Bytes.toBytes(f._2)))
    
	 new RDDHbase(to_put_in).saveAsHbaseTable("categorie")
	 
	              
	// ont récuper jusque ici les 
	
	//val confCassandra = confHbase.set("spark.cassandra.connection.host","localhost")
	
//	val table1 = TableDef.fromType[row]("test", "categorie1")

	//rdd23.saveAsCassandraTableEx(table1, SomeColumns("idcat","name_cat"))
	//rdd23.saveToCassandra("test","categorie",SomeColumns("idcat","name_cat"))
	sc.stop()
	
 	def toString( a: Cell ) : String = {
			return  Bytes.toString(CellUtil.cloneValue(a))
	} 
}
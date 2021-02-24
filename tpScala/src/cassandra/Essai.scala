package cassandra

import org.apache.spark._
import com.datastax.spark.connector._
import org.apache.spark.sql._
import scala.collection.Seq
import scala.reflect.runtime.universe

object Essai extends App {
	org.apache.log4j.Logger.getLogger("org.apache.spark").setLevel(org.apache.log4j.Level.OFF )
	println("1")
	val conf = new SparkConf().setAppName ("Spark␣on␣Cassandra").setMaster("local[*]")
	.set("spark.cassandra.connection.host","localhost")


	val sc = new SparkContext ( conf )
	println("2")
	val rdd = sc.cassandraTable("test","kv")
	println("3")
	println ( rdd.count )
	println ( rdd.first )
	println ( rdd.map ( _.getInt ("value")).reduce (_+_))
	if(rdd.count > 0){
		val rdd2 = sc.parallelize(Seq(("key3" , 3) , ("key4", 4)))
				rdd2.saveToCassandra("test" , "kv" , SomeColumns( "key" , "value"))
	}
	sc.stop ()
}
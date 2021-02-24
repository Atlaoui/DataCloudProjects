package datacloud.exam.nov2018
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
object TestPoint extends App{
	class Point(val x : Int , val y :Int)
case class Point2(val x : Int , val y :Int)

val p = new Point(0,0)
val q = new Point(0,0)
val p2 = new Point2(0,0)
val q2 = new Point2(0,0)
	
	println(p == q )
	println(p2 == q2)

	def f(fichier :Seq[String] , outdir:String) ={
	  val sc = new SparkContext(new SparkConf().setAppName("appli").setMaster("yarn"))
	  val a = fichier.map(fich => sc.textFile(fich).flatMap(_.split(" "))
	                                                .filter(_.length()>2)
	                                                .distinct(2)
	                                                .map((_,fich))
	                        )
	   val b = a.reduce(_ union _)
	   val c = b.groupByKey(3)
	   val d = c.sortByKey(true, 1)
	   d.saveAsTextFile(outdir)
	}
	
	
}
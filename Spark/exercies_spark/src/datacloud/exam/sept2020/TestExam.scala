package datacloud.exam.sept2020
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel

object TestExam extends App{
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  val path = "/home/adrien/SAR2_Assignments/DataCloud/TME/Spark/exercies_spark/src/datacloud/exam/sept2020"
	var langGuesser = new LanguageGuesser( Map("eng" -> (path+"/data/dicoEng.txt"), "fr" -> (path+"/data/dicoFr.txt")))
	  //val sc = new SparkContext(new SparkConf().setAppName("LanguageGuesser").setMaster("local[*]"))
   
	  println(langGuesser.devineLanguage(path+"/data/textfile.txt"))
	  
}

		/*	val rdd1 = sc.parallelize(Seq("a", "b", "b", "c", "c", "c", "c"))
			val rdd2 = sc.parallelize(Seq("a", "a", "b", "c", "c"))

			val cogrouped = rdd1.map(k => (k, null)).cogroup(rdd2.map(k => (k, null)))
			val groupSize = cogrouped.map { case (key, (buf1, buf2)) => (key, math.min(buf1.size, buf2.size)) }
	var finalSet = groupSize.flatMap { case (key, size) => List.fill(size)(key) }*/
	//finalSet.collect = Array(a, b, c, c)
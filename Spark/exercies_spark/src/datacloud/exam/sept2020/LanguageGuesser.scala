package datacloud.exam.sept2020
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
/** Q1 **/
class  LanguageGuesser private (){
  
  private val sc = new SparkContext(new SparkConf().setAppName("LanguageGuesser").setMaster("local[*]"))
 private var  dicos :Map[String,RDD[String]]  = null// tmpDico.toMap
  
 def this (m : Map[String,String]) {
    this()     
  var tmpDico = ListBuffer[(String,RDD[String])]();
  for ((k,v) <- m ){
   var file = sc.textFile(v).flatMap(_.split(" "))
   tmpDico+= ((k,file))
  }
  dicos = tmpDico.toMap
  
  }
   
   /** Q2 **/
   private def nbOccur(path :String) : RDD[(String, Int)] = {
     //les mots du text
     val  words = sc.textFile(path).flatMap(_.split(" "))
      words.map(f => (f,1)).reduceByKey(_+_)
   }
   
   
   /** Q3 **/
   private def getNumWordsInDico(path :String , rddMots : RDD[String]) : Int = {
     //les mots du text
     val  words = sc.textFile(path).flatMap(_.split(" "))
     //mots distinct
      words.intersection(rddMots).map(elem => 1).reduce(_+_)
   }
   
   /** Q2-Q3 **/
   private def nbOccur(path :String , rddMots : RDD[String]) : Int = {
     //les mots du text
     val  words = sc.textFile(path).flatMap(_.split(" "))  
     //mots distinct
     val motsInDico = words.intersection(rddMots).map(elem => (elem,1))  
     //ont prend que les doublon
     val doublon = words.map(f=>(f,1)).reduceByKey(_+_).filter(f => f._2>1).map(f => (f._1,f._2-1))
    motsInDico.union(doublon).map(f=> f._2).sum().toInt
   }
   
   /** Q4 **/
   def devineLanguage(textfile:String):String  ={
      var nbParDico = ListBuffer[(String,Int)]();
     for (elem <- dicos){
        nbParDico.append((elem._1, nbOccur(textfile,elem._2)))
     }
     nbParDico.maxBy(f => f._2)._1
   }
}
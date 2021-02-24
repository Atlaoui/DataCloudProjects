

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
import org.apache.spark.rdd._
import hbase.SparkHbase.RDDHbase
case class IDCompte(value :String) 
case class IDOpe(value : String)
case class IDClient(value : String)
import hbase.SparkHbase.MySparkContext
object BankClientExamen2019 {
    
  
  implicit def LongToByteArray(l : Long): Array[Byte] = Bytes.toBytes(1)
  implicit def StringToByteArray(s : String) : Array[Byte] = s.getBytes
   // Question 1
  
   // Question 2
  def loadOperations(sc : SparkContext ) : RDD[(IDOpe, String , Double, IDCompte , String)] = {
    val scH =	 new MySparkContext(sc)
	  val scans = new Scan() 
	  val rdd = scH.hbaseTableRDD("operation", scans)
	  return rdd.map(f => (IDOpe( Bytes.toString(f._2.getValue("defaultcf".getBytes, "idop".getBytes))),
	                 Bytes.toString(f._2.getValue("defaultcf".getBytes, "date".getBytes)),
                   Bytes.toDouble(f._2.getValue("defaultcf".getBytes, "montant".getBytes)),
                   IDCompte( Bytes.toString(f._2.getValue("defaultcf".getBytes, "compte".getBytes))),
                   Bytes.toString(f._2.getValue("defaultcf".getBytes, "comm".getBytes))
                   ))
                  
  }
  
  def loadComptes(sc : SparkContext ) : RDD[(IDCompte, String , String, IDClient)] = {
    val scH =	 new MySparkContext(sc)
	  val scans = new Scan() 
	  val rdd = scH.hbaseTableRDD("compte", scans)
    return rdd.map(f => (IDCompte( Bytes.toString(f._2.getValue("defaultcf".getBytes, "idcpt".getBytes))),
                          Bytes.toString(f._2.getValue("defaultcf".getBytes, "date_creation".getBytes)),
                          Bytes.toString(f._2.getValue("defaultcf".getBytes, "type".getBytes)),
                          IDClient( Bytes.toString(f._2.getValue("defaultcf".getBytes, "client".getBytes)))
                          ))
  }

  def loadClients(sc : SparkContext ) : RDD[(IDClient, String , String)] = {
    val scH =	 new MySparkContext(sc)
	  val scans = new Scan() 
	  val rdd = scH.hbaseTableRDD("client", scans)
    return rdd.map(f => (IDClient( Bytes.toString(f._2.getValue("defaultcf".getBytes, "idclient".getBytes))),
                          Bytes.toString(f._2.getValue("defaultcf".getBytes, "nom".getBytes)),
                          Bytes.toString(f._2.getValue("defaultcf".getBytes, "adresse".getBytes))
                          ))
  }

   // Question 3
  def soldeTotalParClient() : RDD[(IDClient,Double)] = {
    val spC = new SparkConf()
	  val conf = spC.setAppName ("").setMaster("local[*]")
	  val sc = new SparkContext(conf)

    val rdd_compte = loadComptes(sc)
    val rdd_clients = loadClients(sc)
    val rdd_operations = loadOperations(sc)
    
    val rdd_compte_solde = rdd_operations.map(f => (IDCompte(f._5),f._3)).reduceByKey(_+_)
    
    
    val rdd_compte_clt = rdd_compte.map(f => (f._1,f._4))
    val rdd_compte_sold_client = rdd_compte_solde.join(rdd_compte_clt)
    
    val rdd_client_solde = rdd_compte_sold_client.map(f => (f._2._2 , f._2._1)).reduceByKey(_+_)
    
    sc.stop()
    
    return rdd_client_solde
  }
  
  // Question 4 
  // associer a chaque compte sont solde nous permetrais de retirer le coup des deux jointure
  //et pour chaque ajout d'operation il faudra update le compte en question
  
    def loadComptesNormalized(sc : SparkContext ) : RDD[(IDCompte, String , String, IDClient, Double)] = {
    val scH =	 new MySparkContext(sc)
	  val scans = new Scan() 
	  val rdd = scH.hbaseTableRDD("compte", scans)
    return rdd.map(f => (IDCompte( Bytes.toString(f._2.getValue("defaultcf".getBytes, "idcpt".getBytes))),
                          Bytes.toString(f._2.getValue("defaultcf".getBytes, "date_creation".getBytes)),
                          Bytes.toString(f._2.getValue("defaultcf".getBytes, "type".getBytes)),
                          IDClient( Bytes.toString(f._2.getValue("defaultcf".getBytes, "client".getBytes))),
                          Bytes.toDouble(f._2.getValue("defaultcf".getBytes, "solde".getBytes))     
                   ))
  }
  
  
  def soldeTotalParClientNrmalisation() : RDD[(IDClient,Double)] = {
    val spC = new SparkConf()
	  val conf = spC.setAppName ("").setMaster("local[*]")
	  val sc = new SparkContext(conf)
	  
    val rdd_compte = loadComptesNormalized(sc)
    
    val rdd_client_solde = rdd_compte.map(f => (f._4,f._5)).reduceByKey(_+_)
    
    sc.stop()
    return rdd_client_solde
  }
  // Question 5
  def sortAndsaveClientToHbase(rdd : RDD[(IDClient,Double)]){
    // pas sur si le zipWithIndex préserve l'ordre
    val sorted_rdd_with_rank = rdd.sortBy(f => f._2, true).zipWithIndex
    
    val to_put_in = sorted_rdd_with_rank.map(f => new Put(f._1._1.value).addColumn("defaultcf", "rank", f._2))
     
    new RDDHbase(to_put_in).saveAsHbaseTable("client")
    
  }

}
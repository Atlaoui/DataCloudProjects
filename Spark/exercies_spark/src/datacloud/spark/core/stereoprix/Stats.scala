package datacloud.spark.core.stereoprix
import org.apache.spark.{SparkConf, SparkContext}
object Stats {
	//data : JJ_MM_AAAA_hh_mm magasin prix produit categorie
	
			def chiffreAffaire(ad : String ,anee : Int) : Int = {
			  val conf = new SparkConf().setAppName("Chiffre").setMaster("local[4]")
			val spark = new SparkContext(conf)
					val data = spark.textFile (ad)
					    //On slplit par espace on récupére la date et le prix
					val r= 	data.map( line => line.split(" ")).map( array => (array(0).split("_")(2),array(2)))
							.filter( array => array._1.toInt == anee).map(t => t._2.toInt).reduce(_+_)
							spark.stop()
							r
	}


	def chiffreAffaireParCategorie(ad : String , ad2 : String){
	  val conf = new SparkConf().setAppName("Chiffre").setMaster("local[4]")
			val spark = new SparkContext(conf)
		val data = spark.textFile (ad)
				val rdd = data.map( line => line.split(" ")).map(array => (array(4),array(2))).groupByKey()
				.map {case (key, value) => (key.toString()+":"+value.map(t => t.toInt).reduce(_ + _))}
		rdd.saveAsTextFile(ad2)
		spark.stop()
	}
	def produitLePlusVenduParCategorie(ad : String , ad2 : String){
	  val conf = new SparkConf().setAppName("Chiffre").setMaster("local[4]")
			val spark = new SparkContext(conf)
	  	val data = spark.textFile (ad)
					val rdd=data.map( line => line.split(" ")).map( array => (array(4),array(3))).groupByKey()
					     //map des catégorie et une list (produit)
					    .map {case (key, value) => (key.toString()+":"+value
					        //map de la list qui produit une liste (produit: nb) en prend le max et retourn le nom
					        .map(x => (x, value.count(_== x))).maxBy(_._2)._1)}

			rdd.saveAsTextFile(ad2)
			spark.stop()
	}
}
package datacloud.spark.core.matrix
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.rdd.RDD

import datacloud.scala.tpobject.vector.VectorInt
import org.apache.log4j.Logger
import org.apache.log4j.Level
//http://spark.apache.org/docs/latest/rdd-programming-guide.html
object MatrixIntAsRDD {
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
	implicit def rddToMatrix(l: RDD[VectorInt])= new MatrixIntAsRDD(l)
			def apply(id: RDD[VectorInt]): MatrixIntAsRDD = {
					var t = new MatrixIntAsRDD(id)
							t
	}

	def makeFromFile(url : String , nb : Int ,spark : SparkContext) :MatrixIntAsRDD ={ 
			val rdd1 = spark.textFile(url).map(s => s.split(" ")
					.map(e => e.toInt ))

					//calcule du nombre de partition
					val nb_elem = rdd1.count().toInt
					val nb_partition = if(nb_elem<nb) nb_elem else nb

					//rendue final
					val rdd2=rdd1.map(elem => VectorInt(elem))
					.zipWithIndex().sortBy(pair => pair._2 , true, nb_partition)
					.map(e => e._1)
					new MatrixIntAsRDD(rdd2)
	}
}


class MatrixIntAsRDD (val lines : RDD[VectorInt]) {

	override def toString={
			val sb = new StringBuilder()
					lines.collect().foreach(line => sb.append(line+"\n"))
					sb.toString()
	}

	val nbLines : Long = {lines.count()}

			val nbColumns : Long = { lines.first().length}


	def get(i:Int,j:Int):Int = {
			//trouver une autre méthode
			lines.zipWithIndex().filter(_._2 == i).first()._1.get(j)
	}

	def canEqual(other: Any) = {other.isInstanceOf[datacloud.spark.core.matrix.MatrixIntAsRDD]}

	override def equals(other: Any) = {
			other match {
			case other : MatrixIntAsRDD => { 
				if(other.nbLines == this.nbLines && other.nbColumns == this.nbColumns){
					other.lines.zip(this.lines).filter(tuple => tuple._1 != tuple._1).isEmpty()
				}
				else 
					false
			}
			//case that: datacloud.spark.core.matrix.MatrixIntAsRDD => that.canEqual(MatrixIntAsRDD.this)
			case _ => false
			}
	}


	def +(other : MatrixIntAsRDD) : MatrixIntAsRDD = {
			MatrixIntAsRDD(lines.zip(other.lines).map(f => f._1 + f._2))
	}


	def transpose() : MatrixIntAsRDD = {
	 	val rdd = lines.zipWithIndex()
					.flatMap(vec=>vec._1.elements.zipWithIndex.map(elem=>(elem._2,(vec._2,elem._1)))).groupByKey()
		 			val rdd2 = rdd.mapValues(elem => VectorInt(elem.toArray.sortBy(_._1).map(_._2)))
					.sortBy(elem => elem._1 ,true)
					.map(elem => elem._2)
					MatrixIntAsRDD(rdd2)
	}

	def *(mat : MatrixIntAsRDD) : MatrixIntAsRDD = {
			//- Étape 1 : Coupler la colonne i de la matrice A avec la ligne i de la matrice B

			val coupAB = this.transpose().lines.zip(mat.lines)   

					//- Étape 2 : Appliquer le produit dyadique sur chaque couple

					val prod = coupAB.map(elem=>elem._1.prodD(elem._2))
					.map(elem=>elem.zipWithIndex).flatMap(elem=>elem).map(f=>f.swap)// <- peut mieux faire
					//- Étape 3 : Faire la somme des produits dyadique

					MatrixIntAsRDD(prod.reduceByKey(_+_).map(elem => elem._2))
	}


}
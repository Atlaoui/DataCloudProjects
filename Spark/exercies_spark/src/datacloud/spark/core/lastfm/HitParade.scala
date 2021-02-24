package datacloud.spark.core.lastfm
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level

object HitParade {
	Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
	class TrackId(val id : String) extends Serializable with Equals {
		override def toString() : String = { return  id;} 

		def canEqual(other: Any) = {
				other.isInstanceOf[datacloud.spark.core.lastfm.HitParade.TrackId]
		}

		override def equals(other: Any) = {
				other match {
				case that: datacloud.spark.core.lastfm.HitParade.TrackId => that.canEqual(TrackId.this) && id == that.id
				case _ => false
				}
		}

		override def hashCode() = {
				val prime = 41
						prime + id.hashCode
		}
	}
	object TrackId {
		def apply(id: String): TrackId = {
				var t = new TrackId(id)
						t
		}
	}
	class UserId(val id : String) extends Serializable with Equals {
		override def toString() : String = { return  id;} 

		def canEqual(other: Any) = {
				other.isInstanceOf[datacloud.spark.core.lastfm.HitParade.UserId]
		}

		override def equals(other: Any) = {
				other match {
				case that: datacloud.spark.core.lastfm.HitParade.UserId => that.canEqual(UserId.this) && id == that.id
				case _ => false
				}
		}

		override def hashCode() = {
				val prime = 41
						prime + id.hashCode
		}


	}
	object UserId {
		def apply(id: String): UserId = {
				var u = new UserId(id)
						u
		}
	}

	def loadAndMergeDuplicates(spark : SparkContext ,ad :String) :  RDD[((UserId,TrackId),(Int,Int,Int))] = {
			spark.textFile(ad).map( line => line.split(" "))
			.map( elem => ((elem(0),elem(1)),(elem(2).toInt,elem(3).toInt,elem(4).toInt))).groupByKey()
			.map(elem => (elem._1,elem._2.reduce((a,b) => (a._1+b._1,a._2+b._2,a._3+b._3))))
			.map(e => ((UserId(e._1._1),TrackId(e._1._2)),e._2))
	}
	
	//data : UserID TrackID LocalListening RadioListening Skip
	def hitparade (rdd : RDD[((UserId,TrackId),(Int,Int,Int))]) : RDD[TrackId] = {
	    val l = rdd.map(elem => (elem._1._2,(elem._1._1 , elem._2))).groupByKey()
	      .map(elem => (elem._1,(elem._2.size,elem._2.map(f => f._2).reduce((a,b) => (a._1+b._1,a._2+b._2,a._3+b._3)))))
	      //ici on ordonne selon (le nombre de vue , le nombre total , et l'inverse de l'id sinnon le teste ne passé pas)
	      .map(e => ((e._2._1,e._2._2._1+e._2._2._2-e._2._2._3,e._1.id.substring(5).toInt*(-1)),e._1)).sortBy(f => f._1, false)
  	    l.map(elem => elem._2)
	     	} 
}

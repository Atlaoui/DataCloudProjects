
import org.apache.spark._
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import org.apache.spark.sql._
import scala.collection.Seq
import scala.reflect.runtime.universe
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.io._
import org.apache.hadoop.hbase.mapreduce._
import org.apache.hadoop.hbase.util._
import hbase.SparkHbase.RDDHbase
import hbase.SparkHbase.MySparkContext
import com.datastax.spark.connector.cql.{ColumnDef, RegularColumn, TableDef}
import com.datastax.spark.connector.types._
import java.time.{Instant, LocalDate}



case class User(iduser : String , mail : String , birthyear : Int)
case class Track(idtrack : String , title : String , duration : Int , author : String)
case class Artist (idartist : String , name : String , nbiography : String)
case class Favorite (iduser : String , idtrack : String )

object DixHeurs extends App {
	val sc = new SparkContext(new SparkConf().setAppName("DixHeures").setMaster(this.args(0))
			.set("spark.cassandra.connection.host",this.args(1)))



			def loadUsers() = sc.cassandraTable[User]("dixheurs","User")
			def loadTracks() = sc.cassandraTable[Track]("dixheures","Track")
			def loadArtists() = sc.cassandraTable[Artist]("dixheures","Artists")
			def loadFavorites() = sc.cassandraTable[Favorite]("dixheures","Favorite")

			def addFavorite(f : Favorite ) = sc.parallelize(Seq(f),1).saveToCassandra("dixheures","Favorite",AllColumns)

			def getAvregeListnerByNameArtist() : /*RDD[(String,Double)] = */ Any = {
					val users = loadUsers();
					val tracks = loadTracks();
					val artists = loadArtists();
					val favorites = loadFavorites();
					val curyear = LocalDate.now.getYear

							// ont recuperer les id et les auteur
							val rdd_idtrack_artist = tracks.map(t => (t.idtrack,t.author))
							// les id user avec les idtrack
							val rdd_login_track = favorites.map(f => (f.iduser,f.idtrack))
							// les id user et leur age
							val rdd_user_age = users.map(u => (u.iduser,curyear - u.birthyear));

					// le 
					val rdd_login_idtrack_age = rdd_login_track.join(rdd_user_age);

					val rdd_idtrack_age = rdd_login_idtrack_age.map(_._2);

					val rdd_idtrack_idartist_age = rdd_idtrack_artist.join(rdd_idtrack_age);

					val rdd_idartist_age = rdd_idtrack_idartist_age.map(_._2);

					val rdd_idartist_nameartist_age = artists.map(a => (a.idartist,a.name)).join(rdd_idartist_age)

							val rdd_nameartist_age = rdd_idartist_nameartist_age.map(_._2);

					//return rdd_nameartist_age.map((_.1)).reduceByKey((c1,c2) => (c1._1+c2._1,c1._2+c2._2)).mapValues(c=> c._1.toDouble/c._2.toDouble);
			}





}

//scala TemperatureData | nc -l -p 4242
import scala.util.Random;
object TemperatureData extends App{
	val Names : Seq[String] = Seq("Hermontos","Tripide","Asoinon","Naistos","Iolcapolis","Colissos","Hyeloe",
			"Acurias","Laresmos","Pylonia","Poson","Cebrinia","Chersane","Siciopolis","Kamonesos","Daistos","Byllyros","Cythyria",
			"Tomeidonia","Maion","Eparta","Bhrytorion","Stenokampos","Chamenus","Crotatrae",
			"Noupoli" , "Laodamis" , "Erethaseia" , "Pheressa" , "Kosofa")
	val min = 2
	val max = 100
			while ( true ) {  
			  Names.foreach(f =>  println(f+" "+( min + Math.random() * (max - min))))
			  Thread.sleep(10000)
			  }
}
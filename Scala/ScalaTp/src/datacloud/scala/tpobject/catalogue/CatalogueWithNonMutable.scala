package datacloud.scala.tpobject.catalogue

import scala.collection.immutable

class CatalogueWithNonMutable extends Catalogue {
    var map1: Map[String, Double] = immutable.Map[String, Double]().withDefaultValue(-1)

  override def getPrice(nom: String) : Double = {map1(nom)}
  
   override def removeProduct(nom : String) {
     map1=map1 - nom
   }
   
   override def selectProducts (min : Double , max : Double) : Iterable[String] ={
     //map1 map {case (key, value) => ( if (value > min && value < max)  key )};
     var it = List[String]()
     for((k,v)<- map1){
       if(v > min && v< max ){
         it = it :+ k
       }
     }
      it
   }
   override def storeProduct (nom : String ,  prix : Double ) {
    map1 = map1.updated(nom,prix)
   }
  
}
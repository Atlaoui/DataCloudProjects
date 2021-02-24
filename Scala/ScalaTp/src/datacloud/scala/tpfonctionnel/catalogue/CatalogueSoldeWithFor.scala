package datacloud.scala.tpfonctionnel.catalogue

import datacloud.scala.tpobject.catalogue.CatalogueWithNonMutable

class CatalogueSoldeWithFor extends CatalogueWithNonMutable with CatalogueSolde{
  override def solde(pourcent: Int) = {
    //map1 = map1.map{case (key, value) => (key, value - (value * pourcent))}
    for ((key,value) <- map1) {
        map1 = map1.updated(key, value - (value * pourcent))
    }
  }
}

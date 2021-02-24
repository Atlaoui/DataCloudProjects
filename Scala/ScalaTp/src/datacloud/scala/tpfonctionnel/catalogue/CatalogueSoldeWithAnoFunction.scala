package datacloud.scala.tpfonctionnel.catalogue

import datacloud.scala.tpobject.catalogue.CatalogueWithNonMutable

class CatalogueSoldeWithAnoFunction extends CatalogueWithNonMutable with CatalogueSolde {
  override def solde(pourcent: Int) = {
    //La méthode solde sera codée avec l’appel à
    //la méthode mapValues de la classe Map avec un paramétrage d’une fonction anonyme.
    map1 = map1.mapValues(( a : Double) => a-(pourcent * a)).toMap
  }
}

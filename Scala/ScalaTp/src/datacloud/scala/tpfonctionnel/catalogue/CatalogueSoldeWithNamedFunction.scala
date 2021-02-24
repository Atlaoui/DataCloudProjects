package datacloud.scala.tpfonctionnel.catalogue

import datacloud.scala.tpobject.catalogue.CatalogueWithNonMutable

class CatalogueSoldeWithNamedFunction extends CatalogueWithNonMutable with CatalogueSolde {
  override def solde(pourcent: Int) = {
    map1 = map1.mapValues(diminution(_,pourcent)).toMap
  }

  def diminution(a:Double, percent:Int):Double = a * ((100.0 - percent) / 100.0)
}

package datacloud.scala.tpfonctionnel.catalogue

import datacloud.scala.tpobject.catalogue.CatalogueWithNonMutable

class CatalogueSoldeWithNamedFunction extends CatalogueWithNonMutable with CatalogueSolde {
    def diminution(a: Double, percent: Int) = a * (100 - percent) / 100
    def solde(reduction: Int): Unit = {
        catalogue = catalogue.mapValues(diminution(_, reduction)).toMap
    }
}
package datacloud.scala.tpfonctionnel.catalogue
import datacloud.scala.tpobject.catalogue.CatalogueWithNonMutable

class CatalogueSoldeWithAnoFunction extends CatalogueWithNonMutable  with CatalogueSolde {
    def solde(reduction: Int): Unit = {
        catalogue = catalogue.mapValues((v) => (v-(v * reduction/100))).toMap
    }
}

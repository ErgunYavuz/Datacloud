package datacloud.scala.tpfonctionnel.catalogue
import datacloud.scala.tpobject.catalogue.CatalogueWithNonMutable
class CatalogueSoldeWithFor  extends CatalogueWithNonMutable with CatalogueSolde {
    def solde(reduction: Int): Unit = {
        for ((k,v) <- catalogue){
            this.storeProduct(k, v-(v * reduction/100))
        }
    }
}

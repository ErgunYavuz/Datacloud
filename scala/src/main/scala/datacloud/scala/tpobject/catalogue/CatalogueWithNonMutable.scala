package datacloud.scala.tpobject.catalogue

class CatalogueWithNonMutable extends Catalogue {

  var catalogue = scala.collection.immutable.Map[String, Double]()

  def getPrice(nom: String): Double =
    catalogue.getOrElse(nom, -1.0)

  def removeProduct(nom: String): Unit =
    catalogue -= nom

  def selectProducts(min: Double, max: Double): Iterable[String] =
    catalogue.filter(x => x._2 >= min && x._2 <= max).keySet

  def storeProduct(nom: String, prix: Double): Unit =
    catalogue += (nom->prix)
}
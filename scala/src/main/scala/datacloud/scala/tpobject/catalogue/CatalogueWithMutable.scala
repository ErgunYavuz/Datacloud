package datacloud.scala.tpobject.catalogue

class CatalogueWithMutable extends Catalogue {
  var catalogue = scala.collection.mutable.Map[String, Double]()

  def getPrice(nom: String): Double = {
    catalogue.getOrElse(nom, -1.0)
  }

  def removeProduct(nom: String): Unit =
    catalogue.remove(nom)

  def selectProducts(min: Double, max: Double): Iterable[String] = {
    catalogue.filter(x => x._2 >= min && x._2 <= max).keySet
  }

  def storeProduct(nom: String, prix: Double): Unit =
    catalogue.put(nom, prix)
}

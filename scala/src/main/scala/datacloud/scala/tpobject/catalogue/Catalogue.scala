package datacloud.scala.tpobject.catalogue

trait Catalogue {
  def getPrice(nom : String) : Double
  def removeProduct(nom : String) : Unit
  def selectProducts(min : Double, max : Double) : Iterable[String]
  def storeProduct(nom : String, prix : Double): Unit
}
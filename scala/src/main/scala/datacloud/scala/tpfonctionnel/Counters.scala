package datacloud.scala.tpfonctionnel

object Counters {
    def nbLetters(liste:List[String]):Int ={
        val listeMot = liste.flatMap(_.split(" "))
        val listeNB = listeMot.map(_.length)
        val nb = listeNB.reduce(_+_)
        nb
    }
}

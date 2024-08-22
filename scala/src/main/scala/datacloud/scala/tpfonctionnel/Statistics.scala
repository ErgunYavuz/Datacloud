package datacloud.scala.tpfonctionnel

object Statistics {
    def average(notes:List[(Double,Int)]):Double={
        val notesCoef = notes.map(x => (x._1*x._2, x._2))
        val note = notesCoef.reduce((x,y) => (x._1+y._1, x._2+y._2))
        note._1/note._2
    }
}

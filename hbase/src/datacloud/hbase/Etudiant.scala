package datacloud.hbase

import scala.collection.mutable._
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.immutable

final case class Etudiant(nom : String, prenom : String, age : Int, notes :  immutable.Map[String,Int])

object Etudiant {

  def toHbaseObject(e: Etudiant) : Map[(String,String), Array[Byte]] = {

    val map: Map[(String, String),Array[Byte]] = new HashMap[(String, String), Array[Byte]]()

    // ColFam1 : info possède 3 colonnnes qualifier : nom, prenom, age
    val colfam1 = "info"
    map += (colfam1, "nom") -> Bytes.toBytes(e.nom)
    map += (colfam1, "prenom") -> Bytes.toBytes(e.prenom)
    map += (colfam1, "age") -> Bytes.toBytes(e.age)

    // ColFam2 : notes possède une colonne qualifier par matière
    val colfam2 = "notes"
    for((matiere, note) <- e.notes) {
      map += (colfam2, matiere) -> Bytes.toBytes(note)
    }
    map
  }

  def HbaseObjectToEtudiant(d : Map[(String,String),Array[Byte]]) : Etudiant = {
    
    var nom = ""
    var prenom = ""
    var age = -1
    var notes = immutable.Map[String, Int]()

    // colsFQ : (String, String) , value : Array[Byte]
    for( (colsFQ, value) <- d) {
      colsFQ._1 match {
        case "info" =>
          colsFQ._2 match {
            case "nom" => nom = new String(value)
            case "prenom" => prenom = new String(value)
            case "age" => age = Bytes.toInt(value)
          }

        case "notes" => 
          notes += colsFQ._2 -> Bytes.toInt(value)
      }
    }
    Etudiant(nom, prenom, age, notes)
  }
}
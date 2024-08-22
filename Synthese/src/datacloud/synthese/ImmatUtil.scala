package datacloud.synthese

import com.datastax.oss.driver.api.core.CqlSession
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util._
import org.apache.spark._
import org.apache.spark.sql._
import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

object ImmatUtil {

    case class Immatriculation(numero: String, date_mise_circu: Int, idmodele: String, proprios: Map[Int, String], statut: String)
    case class Modelevehicule(id: String, annee_sortie: Int, marque: String, modele: String)

    def changeAdresse(id: String, addr: String, sc:SparkContext) = {
        val cqlsession= CqlSession.builder().withKeyspace("immat").build();
        val statement = "UPDATE proprietaire SET adresse = ? WHERE id = ?"
        cqlsession.execute(cqlsession.prepare(statement).bind(addr, id))
        cqlsession.close()
    }

    def topThree(annee: Int, sc: SparkContext) = {
        val immmatriculation = sc.cassandraTable[Immatriculation]("immat", "immatriculation").map(row => (row.idmodele, row.date_mise_circu))
        val modelevehicule = sc.cassandraTable[Modelevehicule]("immat", "modelevehicule").map(row => (row.id, row.marque))
        immmatriculation.join(modelevehicule).map(row => row._2).filter(row => row._1 == annee).map(row => (row._2, 1)).reduceByKey(_+_).sortBy(x=>x._2,false).map(x=>x._1).take(3)
    }

    def fillTracingTable(log: DStream[String], tn: TableName, connexion: Connection, sc: SparkContext) = {
        val logs = log.map(_.split(" "))
        //namespace: vols
        //qualifier: tracagevoiture

    }
}

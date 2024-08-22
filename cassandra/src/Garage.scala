import com.datastax.spark.connector._
import org.apache.spark._
import org.apache.spark.sql._
import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}

case class Mecano(idmecano: Int, nom: String, prenom: String, status: String)
case class Mecanicien(idmecano: Int, nom: String, prenom: String, status: String)
case class Vehicule(idvehicule: Int, marque: String, modele: String, kilometrage: Int, mecano: Int)
case class Reparation(idvehicule: Int, marque: String, modele: String, kilometrage: Int, idmecano: Int, nom: String, prenom: String, status: String)

object Copy extends App {
    // Connection to Cassandra
    org.apache.log4j.Logger.getLogger("org.apache.spark").setLevel(org.apache.log4j.Level.OFF)
    val conf = new SparkConf().setAppName("Spark on Cassandra").setMaster("local[*]").set("spark.cassandra.connection.host", "localhost")
    val sc = new SparkContext(conf)
    val mecanicien = sc.cassandraTable("garage", "mecanicien")
    val mecanicien_cpy = TableDef.fromType[Mecano]("garage", "mecanicien_cpy")
    mecanicien.saveAsCassandraTableEx(mecanicien_cpy, AllColumns)
    sc.stop()
}

object Reparation extends App {
    org.apache.log4j.Logger.getLogger( "org.apache.spark").setLevel(org.apache.log4j.Level.OFF)
    val conf = new SparkConf().setAppName("Reparation").setMaster("local[*]").set("spark.cassandra.connection.host" ,"localhost")
    val sc = new SparkContext(conf)

    val mecanicien = sc.cassandraTable[Mecanicien]("garage", "mecanicien").map(row => (row.idmecano, row))
    val vehicule = sc.cassandraTable[Vehicule]("garage", "vehicule").map(row => (row.mecano, row))
    val RDDreparation = mecanicien.join(vehicule).map(_._2).map(row => Reparation
    (row._2.idvehicule, row._2.marque, row._2.modele, row._2.kilometrage,
        row._2.mecano, row._1.prenom, row._1.nom, row._1.status))
    val reparation = TableDef.fromType[Reparation]("garage", "reparation")
    RDDreparation.saveAsCassandraTableEx(reparation,AllColumns)
    sc.stop
}

object AddCol extends App {
    org.apache.log4j.Logger.getLogger("org.apache.spark").setLevel(org.apache.log4j.Level.OFF)
    val conf = new SparkConf().setAppName("Spark on Cassandra").setMaster("local[*]").set("spark.cassandra.connection.host", "localhost")
    val sc = new SparkContext(conf)
    CassandraConnector(conf).withSessionDo { session =>
        session.execute("ALTER TABLE garage.mecanicien ADD vehicules list<int>;")
    }
    val rdd_vehc = sc.cassandraTable[Vehicule]("garage", "vehicule").map(r => (r.mecano, r.idvehicule)).groupByKey.map(r => (r._1, r._2.toList))
    rdd_vehc.saveToCassandra("garage", "mecanicien", SomeColumns("idmecano" as "_1", "vehicules" as "_2"))

    sc.stop()
}

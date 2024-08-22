package datacloud.hbase

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable
import scala.collection.mutable.Map
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.util.control.Breaks.break

object Stereoprix {
    def nbVenteParCategorie: scala.collection.immutable.Map[String, Int] = {
        //configuration
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", "localhost")
        val c = ConnectionFactory.createConnection(conf)
        val namespace="stereoprix"
        val cf = Bytes.toBytes("defaultcf")

        //connexion au tables
        val categories_table = c.getTable(TableName.valueOf(namespace,"categorie"))
        val produits_table = c.getTable(TableName.valueOf(namespace,"produit"))
        val ventes_table = c.getTable(TableName.valueOf(namespace,"vente"))

        //creations des map des tables
        val categories_map = Map[String, String]()
        val produits_map = Map[String, String]()
        val ventes_map = Map[String, Int]()

        //recuperation de la table ventes dans
        for (result <- ventes_table.getScanner(new Scan).asScala){
            val k = new String(result.getValue(cf,Bytes.toBytes("produit")))
            val v = ventes_map.getOrElse(k,0)+1
            ventes_map+= k -> v
        }
        //recuperation de la table produits
        for (result <- produits_table.getScanner(new Scan).asScala) {
            val k = new String(result.getValue(cf, Bytes.toBytes("idprod")))
            val v = new String(result.getValue(cf, Bytes.toBytes("categorie")))
            produits_map += (k -> v)
        }
        //recuperation de la table categorie
        for (result <- categories_table.getScanner(new Scan).asScala) {
            val k = new String(result.getValue(cf, Bytes.toBytes("idcat")))
            val v = new String(result.getValue(cf, Bytes.toBytes("designation")))
            categories_map += (k -> v)
        }

        //jointure vente categorie sur productId
        val categories_ventes= ventes_map.flatMap { case (productId, saleNumber) =>
            produits_map.get(productId).map(categoryId => (productId, saleNumber, categoryId))
        }.groupBy(_._3).mapValues(_.map(_._2).sum).toMap

        //jointure categorie_vente categorie sur categoryId
        val designation_ventes = categories_ventes.flatMap{ case (categoryId, saleNumber) =>
            categories_map.get(categoryId).map(designation => (designation, saleNumber))
        }
        designation_ventes
    }

    def denormalise() = {
        //configuration
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", "localhost")
        val c = ConnectionFactory.createConnection(conf)
        val namespace = "stereoprix"
        val cf = Bytes.toBytes("defaultcf")

        //connexion aux tables
        val ventes_table = c.getTable(TableName.valueOf(namespace,"vente"))

        //recuperation de la table produits
        val produits_table = c.getTable(TableName.valueOf(namespace,"produit"))
        val produits_map = Map[String,String]()
        for (result <- produits_table.getScanner(new Scan).asScala) {
            val k = new String(result.getValue(cf, Bytes.toBytes("idprod")))
            val v = new String(result.getValue(cf, Bytes.toBytes("categorie")))
            produits_map += (k -> v)
        }

        // recuperation de la table categorie
        val categories_map = Map[String, String]()
        val categories_table = c.getTable(TableName.valueOf(namespace,"categorie"))
        for (result <- categories_table.getScanner(new Scan).asScala) {
            val k = new String(result.getValue(cf, Bytes.toBytes("idcat")))
            val v = new String(result.getValue(cf, Bytes.toBytes("designation")))
            categories_map += (k -> v)
        }

        //jointure vente categorie sur productId
        val produit_designation = produits_map.flatMap { case (productId, categorieId) => categories_map.get(categorieId).map(designation => (productId, designation))}

        val scan = new Scan()
        scan.addColumn(cf,Bytes.toBytes("rowkey"))
        scan.addColumn(cf,Bytes.toBytes("produit"))
        val res = ventes_table.getScanner(scan).asScala
        for (result <- res){
            val put = new Put(result.getRow)
            val produit = new String(result.getValue(cf,Bytes.toBytes("produit")))
            put.addColumn(cf, Bytes.toBytes("designation"), Bytes.toBytes(produit_designation(produit)))
            ventes_table.put(put)
        }
    }

    def nbVenteParCategorieDenormalise:collection.immutable.Map[String,Int] = {
        //configuration
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", "localhost")
        val c = ConnectionFactory.createConnection(conf)
        val namespace = "stereoprix"
        val cf = Bytes.toBytes("defaultcf")
        //connexion aux tables
        val ventes_table = c.getTable(TableName.valueOf(namespace, "vente"))

        val ventes_map = Map[String,Int]()
        for (result <- ventes_table.getScanner(new Scan).asScala){
            val k = new String(result.getValue(cf, Bytes.toBytes("designation")))
            val v = ventes_map.getOrElse(k, 0) + 1
            ventes_map += k -> v
        }

        ventes_map.groupBy(_._1).mapValues(_.map(_._2).sum).toMap
    }

    def addVente(c:Connection,idvente:String,idclient:String,idmag:String,idprod:String,date:String):Unit = {
        val namespace="stereoprix"
        val cf = Bytes.toBytes("defaultcf")

        val categorieTable = c.getTable(TableName.valueOf(namespace, "categorie"))
        val produits_table = c.getTable(TableName.valueOf(namespace, "produit"))
        val venteTable = c.getTable(TableName.valueOf(namespace, "vente"))

        val prodget = new Get(Bytes.toBytes(idprod))
        val prod = produits_table.get(prodget)
        val cat = prod.getValue(cf, Bytes.toBytes("categorie"))

        val getCat = new Get(cat)
        val resultCat = categorieTable.get(getCat)
        val catdesignation = resultCat.getValue(cf, Bytes.toBytes("designation"))

        val put = new Put(Bytes.toBytes(idvente))
        put.addColumn(cf, Bytes.toBytes("client"), Bytes.toBytes(idclient))
        put.addColumn(cf, Bytes.toBytes("produit"), Bytes.toBytes(idprod))
        put.addColumn(cf, Bytes.toBytes("magasin"), Bytes.toBytes(idmag))
        put.addColumn(cf, Bytes.toBytes("date"), Bytes.toBytes(date))
        put.addColumn(cf, Bytes.toBytes("designation"), catdesignation)

        venteTable.put(put)
        venteTable.close()
        produits_table.close()
        categorieTable.close()
    }
}


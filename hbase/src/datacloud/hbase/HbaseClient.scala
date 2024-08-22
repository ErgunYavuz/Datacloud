package datacloud.hbase
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase._

import collection.mutable._
import org.apache.hadoop.hbase.util._
import scala.jdk.CollectionConverters.MapHasAsScala

class HbaseClient(c:Connection) {

    def createTable(tn: TableName, colfams: String*) = {
        //check namespace
        val name = tn.getNamespaceAsString
        val namespaces = c.getAdmin.listNamespaces()
        namespaces.contains(name) match {
            // create namespace
            case false => val namespace = NamespaceDescriptor.create(name).build
                c.getAdmin.createNamespace(namespace)
            case _ =>
        }
        //check table
        if (!c.getAdmin.tableExists(tn)) {
            //create table constructor
            val tdb: TableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tn);
            colfams.foreach { cf =>
                tdb.setColumnFamily(
                    ColumnFamilyDescriptorBuilder.newBuilder(cf.getBytes()).build())
            }
            val td = tdb.build()
            //create table
            c.getAdmin.createTable(td)
        }
    }

    def deleteTable(tn: TableName): Unit = {
        if (c.getAdmin.tableExists(tn)) {
            //delete table
            c.getAdmin.disableTable(tn)
            c.getAdmin.deleteTable(tn)
        }
        //check namespace
        val name = tn.getNamespaceAsString
        val namespaces = c.getAdmin.listNamespaces()
        namespaces.contains(name) match {
            case true =>
                c.getAdmin.listTableNamesByNamespace(name).isEmpty match {
                    case true => c.getAdmin.deleteNamespace(tn.getNamespaceAsString)
                    case _ =>
                }
            case _ =>
        }
    }

    def writeObject[E](tn: TableName, rowkey : Array[Byte], obj : E)(implicit convert : E =>Map[(String,String),Array[Byte]])= {
            c.getAdmin.tableExists(tn) match {
                //si la table n'existe pas
                case false => throw new Exception("HbaseClient.writeObject : La table n'existe pas")
                case true => {
                    //connexion a la table
                    val table = c.getTable(tn)
                    val put = new Put(rowkey)
                    val rowObj = convert(obj)
                    rowObj.foreach(r => put.addColumn(r._1._1.getBytes, r._1._2.getBytes, r._2))
                    table.put(put)
                }
            }
        }

    def readObject[E](tn: TableName, rowkey: Array[Byte])(implicit conv: Map[(String, String), Array[Byte]] => E): Option[E] = {

        if (c.getAdmin.tableExists(tn)) {

            // connection à la table tn
            val table = c.getTable(tn)
            val get: Get = new Get(rowkey)
            // envoi de la requete sur la table
            val res: Result = table.get(get)
            //NavigableMap[Array[Byte],NavigableMap[Array[Byte],Array[Byte]]] : Map<ColumnFamily, Map<ColumnQualifier, valeur >>
            val mapNoVersionRes = res.getNoVersionMap()
            // mutable.Map[Array[Byte],mutable.Map[Array[Byte],Array[Byte]]]
            val mapasScala = mapNoVersionRes.asScala.map(cf => (cf._1, cf._2.asScala))

            var mapRes: Map[(String, String), Array[Byte]] = Map[(String, String), Array[Byte]]()

            for ((colfam, valeur) <- mapasScala) {
                val colF = Bytes.toString(colfam)
                for ((colQualifier, value) <- valeur) {
                    val colQ = Bytes.toString(colQualifier)

                    mapRes = mapRes + ((colF, colQ) -> value)
                }
            }

            return Some(conv(mapRes))

        } else {
            // la table n'existe pas
            throw new Exception("HbaseClient.readObject : La table n'existe pas")
        }

    }


}


package datacloud.synthese

import java.io.File
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.io._
import org.apache.hadoop.hbase.mapreduce._
import org.apache.hadoop.hbase.util._
import org.apache.spark._
import org.apache.spark.rdd._

import java.util.ArrayList

object LastFmUtil {
    //UserId TrackId LocalListening RadioListening Skip
    /*
    def fillfromFile(log: File, tablename:TableName, cf: String, sc: SparkContext) = {
        val cfbytes = Bytes.toBytes(cf)
        val userid = Bytes.toBytes("userid")
        val trackid = Bytes.toBytes("trackid")
        val locallistening = Bytes.toBytes("locallistening")
        val radiolistening = Bytes.toBytes("radiolistening")
        val skip = Bytes.toBytes("skip")

        val rdd1 = sc.textFile(log.getAbsolutePath)
        val rdd2 = rdd1.map(_.split(" "))
        val rdd3 = rdd2.map(x => (x(0), x(1), x(2), x(3), x(4), x(5)))
        val rddMut:RDD[Mutation] = rdd3.map(fields => {
            val put = new Put(Bytes.toBytes(fields._1+fields._2))
            put.addColumn(cfbytes, userid, Bytes.toBytes(fields._1))
            put.addColumn(cfbytes, trackid, Bytes.toBytes(fields._2))
            put.addColumn(cfbytes, locallistening, Bytes.toBytes(fields._3))
            put.addColumn(cfbytes, radiolistening, Bytes.toBytes(fields._4))
            put.addColumn(cfbytes, skip, Bytes.toBytes def fillfromFile(log: File, tablename:TableName, cf: String, sc: SparkContext) = {
        val cfbytes = Bytes.toBytes(cf)
        val userid = Bytes.toBytes("userid")
        val trackid = Bytes.toBytes("trackid")
        val locallistening = Bytes.toBytes("locallistening")
        val radiolistening = Bytes.toBytes("radiolistening")
        val skip = Bytes.toBytes("skip")

        val rdd1 = sc.textFile(log.getAbsolutePath)
        val rdd2 = rdd1.map(_.split(" "))
        val rdd3 = rdd2.map(x => (x(0), x(1), x(2), x(3), x(4), x(5)))
        val rddMut:RDD[Mutation] = rdd3.map(fields => {
            val put = new Put(Bytes.toBytes(fields._1+fields._2))
            put.addColumn(cfbytes, userid, Bytes.toBytes(fields._1))
            put.addColumn(cfbytes, trackid, Bytes.toBytes(fields._2))
            put.addColumn(cfbytes, locallistening, Bytes.toBytes(fields._3))
            put.addColumn(cfbytes, radiolistening, Bytes.toBytes(fields._4))
            put.addColumn(cfbytes, skip, Bytes.toBytes(fields._5))
        })
        //rdd to table
        val rddHbase = new RDDHbase(rddMut)
        rddHbase.saveAsHbaseTable(tablename.getNameAsString)
    }(fields._5))
        })
        //rdd to table
        val rddHbase = new RDDHbase(rddMut)
        rddHbase.saveAsHbaseTable(tablename.getNameAsString)
    }

     */
    def fillfromFile(fichier: File, tablename: TableName, cf: String, sc: SparkContext) = {
        val textFile = sc.textFile(fichier.getAbsolutePath)
        val rdd1 = textFile.map(_.split(" "))
        val rdd2 = rdd1.map(x => ((x(0), x(1)), x(2).toLong, x(3).toLong, x(4).toLong))
        val rdd3 = rdd2.groupBy(_._1).map(x => (x._1, x._2.map(x => (x._2, x._3, x._4)).reduce((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))))
        val rdd4 = rdd3.map(x => {
            val k = new Put(Bytes.toBytes(x._1._1 + x._1._2))
            k.addColumn(Bytes.toBytes(cf), Bytes.toBytes("userid"), Bytes.toBytes(x._1._1))
            k.addColumn(Bytes.toBytes(cf), Bytes.toBytes("trackid"), Bytes.toBytes(x._1._2))
            k.addColumn(Bytes.toBytes(cf), Bytes.toBytes("locallistening"), Bytes.toBytes(x._2._1))
            k.addColumn(Bytes.toBytes(cf), Bytes.toBytes("radiolistening"), Bytes.toBytes(x._2._2))
            k.addColumn(Bytes.toBytes(cf), Bytes.toBytes("skip"), Bytes.toBytes(x._2._3))
        })
        val rdd5 = SparkHbaseConnector.rddToRDDHbase(rdd4).saveAsHbaseTable(tablename.getNameAsString)
    }

}

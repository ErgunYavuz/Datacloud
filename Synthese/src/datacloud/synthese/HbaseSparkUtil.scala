package datacloud.synthese

import datacloud.hbase.HbaseClient
import datacloud.synthese.SparkHbaseConnector.{rddToRDDHbase, sparkContextToMySparkContext}
import org.apache.hadoop.hbase.{CellUtil, HColumnDescriptor, TableName, client}
import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptorBuilder, Connection, Put, Scan, TableDescriptor, TableDescriptorBuilder}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext

object HbaseSparkUtil {
    def copyTable(src: TableName, dst: TableName, sc: SparkContext, connection: Connection): Unit = {
        val client = new HbaseClient(connection)
        val cfs = connection.getAdmin.getTableDescriptor(src).getColumnFamilies.map(col => col.getNameAsString)
        client.createTable(dst, cfs:_*)
        val rdd = sc.hbaseTableRDD(src.getNameAsString, new Scan())
        val rddMutation = rdd.map(x => x._2).map(x => x.rawCells()).flatMap(x => x).map(res => {
            val put = new Put(CellUtil.cloneRow(res))
            val colfam = CellUtil.cloneFamily(res)
            val qualifier = CellUtil.cloneQualifier(res)
            val value = CellUtil.cloneValue(res)
            put.addColumn(colfam, qualifier, value)
            put
        })
        rddMutation.saveAsHbaseTable(dst.getNameAsString)
    }
}
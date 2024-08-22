package datacloud.hbase
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase._
import java.io._
import collection.mutable._
import org.apache.hadoop.hbase.util._

import java.io.File
import scala.jdk.CollectionConverters.MapHasAsScala


object LastfmFilling {
    def fromFile(data:File, dest:TableName, col_fam:String):Unit = {
        //connection
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", "localhost")
        val c = ConnectionFactory.createConnection(conf)

        val table = c.getTable(dest)
        synchronized{
            //UserId TrackId LocalListening RadioListening Skip
            for (line <- scala.io.Source.fromFile(data).getLines()){
                //processing line
                val fields = line.split(" ")
                val rowkey = Bytes.toBytes(fields(0) + fields(1))
                val UserId = Bytes.toBytes(fields(0))
                val TrackId = Bytes.toBytes(fields(1))
                val LocalListening = fields(2).toLong
                val RadioListening = fields(3).toLong
                val Skip = fields(4).toLong

                //check rowkey exist
                val get = new Get(rowkey)
                val res = table.get(get)

                if (res.isEmpty) {
                    //put
                    val put = new Put(rowkey)
                    put.addColumn(Bytes.toBytes(col_fam), Bytes.toBytes("userid"), UserId)
                    put.addColumn(Bytes.toBytes(col_fam), Bytes.toBytes("trackid"), TrackId)
                    put.addColumn(Bytes.toBytes(col_fam), Bytes.toBytes("locallistening"), Bytes.toBytes(LocalListening))
                    put.addColumn(Bytes.toBytes(col_fam), Bytes.toBytes("radiolistening"), Bytes.toBytes(RadioListening))
                    put.addColumn(Bytes.toBytes(col_fam), Bytes.toBytes("skip"), Bytes.toBytes(Skip))
                    table.put(put)
                }

                else {
                    //increment
                    val inc = new Increment(rowkey)
                    inc.addColumn(Bytes.toBytes(col_fam), Bytes.toBytes("locallistening"), LocalListening)
                    inc.addColumn(Bytes.toBytes(col_fam), Bytes.toBytes("radiolistening"), RadioListening)
                    inc.addColumn(Bytes.toBytes(col_fam), Bytes.toBytes("skip"), Skip)
                    table.increment(inc)
                }
            }
        }
        c.close()
    }
}

/**
 * @author Jonathan Lejeune
 */


package hbase

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.io._
import org.apache.hadoop.hbase.mapreduce._
import org.apache.hadoop.hbase.util._
import org.apache.spark._
import org.apache.spark.rdd._
import java.util.ArrayList

object SparkHbase {
  class MySparkContext(sc:SparkContext){
     def hbaseTableRDD(tablename:String, scan:Scan, quorumzookeeper:String="localhost"):RDD[(ImmutableBytesWritable,Result)]={
       val hbaseconf = HBaseConfiguration.create()
       hbaseconf.set("hbase.zookeeper.quorum", quorumzookeeper);
       hbaseconf.set(TableInputFormat.INPUT_TABLE, tablename);
       hbaseconf.set(TableInputFormat.SCAN, TableMapReduceUtil.convertScanToString(scan));
       return sc.newAPIHadoopRDD(hbaseconf,classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]);
     }
   }
   implicit def sparkContextToMySparkContext(sc:SparkContext):MySparkContext= new MySparkContext(sc)
   
   
   class RDDHbase[M<:Mutation](rdd:RDD[M]){
     def saveAsHbaseTable(tablename:String, quorumzookeeper:String="localhost"):Unit={
       
       rdd.foreachPartition( it => {
           val hbaseconf = HBaseConfiguration.create()
           hbaseconf.set("hbase.zookeeper.quorum", quorumzookeeper)
           val connection = ConnectionFactory.createConnection(hbaseconf)
           val table  = connection.getTable(TableName.valueOf(tablename))
           val puts = new ArrayList[Put]
           val deletes = new ArrayList[Delete]
           for(mut <- it){
             mut match {
               case put:Put => puts.add(put)
               case delete:Delete => deletes.add(delete)
               case append:Append => table.append(append)
               case increment:Increment => table.increment(increment)
               case _ => throw new RuntimeException("Bad type of mutation")
             }
           }
           if(!puts.isEmpty){
             table.put(puts)
           }
           if(!deletes.isEmpty()){
             table.delete(deletes)
           }
           table.close()
           connection.close()
       })  
      }

   }
   implicit def rddToRDDHbase[M<:Mutation](rdd:RDD[M]): RDDHbase[M] = new RDDHbase(rdd)
}
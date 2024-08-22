package datacloud.synthese.tests

import datacloud.hbase.HbaseClient
import datacloud.synthese.HbaseSparkUtil
import scala.util.Random
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.junit.Test
import org.junit.Assert._
import org.apache.hadoop.hbase.TableName
import datacloud.synthese.tests.SyntheseTest._
import scala.collection.JavaConverters._
import org.apache.hadoop.hbase.client.Scan


class CopyTableTest extends SyntheseTest {
  
  
  @Test
  def testc={
    val src=TableName.valueOf("bidon", "essai")
    val client = new HbaseClient(hbaseconnection)
    client.deleteTable(src)
    client.createTable(src, "cf1","cf2","cf3")
    val rand = new Random
    val puts = Range.apply(0, 1000).map(i=>{
      val p = new Put(Bytes.toBytes("row"+i))
      
      p.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes(rand.nextString(5)), Bytes.toBytes(rand.nextString(10)))
      p.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes(rand.nextString(5)), Bytes.toBytes( rand.nextString(10)))
      p.addColumn(Bytes.toBytes("cf3"), Bytes.toBytes(rand.nextString(5)), Bytes.toBytes(rand.nextString(10)))
      p
    })
    
    hbaseconnection.getTable(src).put(puts.asJava)
    testCopy(src, TableName.valueOf(src.getNamespaceAsString, src.getQualifierAsString+"cpy"))
  }
  
  def testCopy(src:TableName,dest:TableName)={
    HbaseSparkUtil.copyTable(src, dest,sparkcontext,hbaseconnection)
    val tablesrc=hbaseconnection.getTable(src)
    val itsrc = tablesrc.getScanner(new Scan).iterator().asScala
    val tabledest=hbaseconnection.getTable(dest)
    val itdest = tabledest.getScanner(new Scan).iterator().asScala
    assertEquals(itsrc.size,itdest.size)
    while(itsrc.hasNext && itdest.hasNext){
      assertArrayEquals(itsrc.next().getRow, itdest.next().getRow)
    }
  }
}

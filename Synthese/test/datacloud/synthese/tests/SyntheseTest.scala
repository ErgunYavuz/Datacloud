package datacloud.synthese.tests

import java.io.File
import java.nio.file.Files



import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.junit.AfterClass
import org.junit.BeforeClass
import org.apache.hadoop.hbase.client.ConnectionFactory
import com.datastax.oss.driver.api.core.CqlSession


abstract class SyntheseTest {
  
}
object SyntheseTest{
  
  val tmpdirectorypath = Files.createTempDirectory("datacloud_hbase_test_dir")
   
   def deleterec(f:File):Unit={
   f match {
     case file if !file.isDirectory() => {}
     case directory =>  directory.listFiles().foreach(deleterec(_))
    
   }
    println("deleting "+f.toString())
    f.delete()
   } 
    
   
   
  @AfterClass
  def afterclass:Unit={ 
   deleterec(new File(tmpdirectorypath.toString()))
  }
  
  
  
  var hbaseconnection:Connection=null;
  var sparkcontext:SparkContext=null;
  
  @BeforeClass
  def deploy():Unit={
    Runtime.getRuntime.addShutdownHook( new Thread(){
      override def run={
         cleanup
      }
    })
    
    val confhbase = HBaseConfiguration.create() 
    confhbase.set("hbase.zookeeper.quorum", "localhost")
    hbaseconnection = ConnectionFactory.createConnection(confhbase)
    val confspark = new SparkConf().setAppName("TP Synthese").setMaster("local[*]")
                    .set("spark.hadoopRDD.ignoreEmptySplits","false")
    sparkcontext=new SparkContext(confspark)
  }
  
  @AfterClass
  def cleanup={
    if(!hbaseconnection.isClosed())
      hbaseconnection.close()
    if(!sparkcontext.isStopped)
        sparkcontext.stop()
  }
}

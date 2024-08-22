package datacloud.synthese.tests

import java.io.DataOutputStream
import java.net.ServerSocket

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.junit.Test

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.querybuilder.QueryBuilder._
import org.apache.spark.streaming.Seconds
import scala.util.Try
import datacloud.synthese.ImmatUtil
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import java.io.PrintStream
import java.io.File
import datacloud.synthese.tests.AbstractTest._
import java.io.PrintWriter
import datacloud.hbase.HbaseClient
import org.apache.hadoop.hbase.client.Scan

import org.junit.Assert._



class  TracingTest extends AbstractTest {
  
  val sparkconf=new SparkConf().setAppName("test immat").setMaster("local[*]")
  
  private case class Imm(numero:String,statut:String)
  private case class Flash(timestamp:String,x:Int,y:Int)
  
  val random = new Random(20)
  val now = System.currentTimeMillis();
  
  
  def autoClose[A <: AutoCloseable, B](resource: A)(code: A => B): Try[B] = {
    val tryResult = Try {code(resource)}
    resource.close()
    if(tryResult.isFailure) fail(tryResult.get.toString())
    return tryResult
  }

  
    
    implicit def toMyRandom(random:Random) = new MyRandom(random)
  class MyRandom(private val random:Random) {
   
    def nextLongBetween(min:Long,max:Long):Long={
      return (random.nextLong()>>>1)%(max - min) + min
    }
    
    def nextLong(mean:Long,stddev:Long):Long=(random.nextGaussian * stddev + mean).toLong
  } 
  
    
    
    
  def generateJourney(maxpoints:Int):Seq[(Long,(Long,Long))]={
    val res = ArrayBuffer[(Long,(Long,Long))]()
    res.append( (now, (random.nextInt(1000),random.nextInt(1000))) )//pos ititial parmis un terrain de 1000x1000 km    
    val addmin:List[(Long,Long)=>Long] = List( _+_,_-_ )
    
    for(i<- 1 to random.nextInt(maxpoints)){
      val prev = res(i-1)
      val diff_time = random.nextLongBetween(10, 30)//temps de capture compris entre 10min et 30min
      val next_timestamp = prev._1 + (diff_time * 60 * 1000)
      //vitesse moyenne de 60 km/h
      val distance_parcourue = (diff_time * random.nextLong(60,10))/60
      val next_direction = random.nextDouble()*(Math.PI/2);
      val next_distx =  (Math.cos(next_direction)*distance_parcourue).toLong
      val next_disty =  (Math.sin(next_direction)*distance_parcourue).toLong
      
      val nextx = addmin(random.nextInt(Int.MaxValue)%2)(prev._2._1,next_distx)
      val nexty = addmin(random.nextInt(Int.MaxValue)%2)(prev._2._2,next_disty)
      
      res.append((next_timestamp,(nextx,nexty)))
    }
    
    return res
  }
  
  
  
  var portserver:Int=0
  
  @Test
  def test:Unit={
    val ssc = new StreamingContext(new SparkContext(sparkconf),Seconds(2))
    
    autoClose(CqlSession.builder().withKeyspace("immat").build()) { cqlsession=>
        val select =  selectFrom("Immatriculation").all()
        val res = cqlsession.execute(select.build()).all().asScala
        val all_imms = res.map(r=>Imm(r.get("numero", classOf[String]), r.get("statut", classOf[String]) ))
        val all_imms_non_detruit = all_imms.filter(_.statut!="detruit")
        val data = all_imms_non_detruit.flatMap(immat => generateJourney(5).map(p => (immat,p)) )
                                       .map(e => (e._2._1,e._1.numero, e._2._2._1,e._2._2._2))
                                       .sortBy(_._2)
        val sleep=20                               
        val emitter = new Thread(()=>{
           autoClose(new ServerSocket(0)){server=>
              portserver=server.getLocalPort
              autoClose(server.accept()){conn=>
                autoClose(new PrintWriter(conn.getOutputStream)){out=>
                    data.foreach(e => { 
                        out.println(e._1+" "+e._2+" "+e._3+" "+e._4)
                        out.flush()
                        Thread.sleep(sleep)
                      })
                }
              }
           }
           
        })                               
       
        emitter.start()
                                       
        autoClose(ConnectionFactory.createConnection(HBaseConfiguration.create() )){ connhbase=>
          val hbaseclient = new HbaseClient(connhbase)
          val tn=TableName.valueOf("vols", "tracagevoiture")
          hbaseclient.deleteTable(tn)
          ImmatUtil.fillTracingTable(ssc.socketTextStream("localhost", portserver), 
                                   tn,
                                   connhbase,
                                   ssc.sparkContext)
          val time_to_wait = (sleep*data.size*1.1).toLong
                   
          ssc.start()
          
          ssc.awaitTerminationOrTimeout(time_to_wait)
          
          ssc.stop(true, true)
          
          val table = connhbase.getTable(tn)
          val scanner = table.getScanner(new Scan)
          val size_table = scanner.asScala.size
          assertNotEquals(0, size_table)
          val immat_vole = all_imms.filter(_.statut=="vole")
          
          immat_vole.foreach(i=>{
            val computed = ImmatUtil.getJourneyOf(i.numero, tn, connhbase)
            
            val expected = data.filter(_._2==i.numero).sortBy(_._1).map(e=>(e._3,e._4))
            
            assertEquals(expected.size,computed.size)
            for(j<- 0 to expected.size-1){
              assertEquals(expected.apply(j),computed.apply(j))
            }
            
          })
          
          
        }
        
        
       
    }
   
 
  }

  
}




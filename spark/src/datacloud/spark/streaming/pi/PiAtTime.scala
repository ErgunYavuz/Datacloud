package datacloud.spark.streaming.pi

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.log4j._

object PiAtTime extends App{
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Pi At Time").setMaster("local[*]")
    val ssc = new StreamingContext(new SparkContext(conf), Seconds(2))
    val lines = ssc.socketTextStream("localhost", 4242)
    
//    val ds = lines.map(_.split(" ")).map(x => (x(0).toDouble, x(1).toDouble))
//    val ds1 = ds.map(x => if (x._1*x._1+x._2*x._2 < 1) (1) else (0))
//    val ds2 = ds1.reduce(_+_).map(x => (x,ds1.count()))
//    ds2.print()
//    val ds3 = ds1.filter(x => x == 1)
//    val ds4 = ds3.reduce(_+_)
  
 
  val split = lines.map(_.split(" "))
  val doubles = split.map{word => if (word(0).toDouble*word(0).toDouble+word(1).toDouble*word(1).toDouble < 1 ) 1 else 0 }
  val res = doubles.reduce(_+_).map(v => (1, 4.0 * v)).join(lines.count().map(v => (1, v))).map(x => x._2._1/x._2._2)
  res.print()
  
  ssc.start()
  println("*** Pi At Time ***")
  ssc.awaitTermination()
}
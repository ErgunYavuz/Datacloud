package revision


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import revision.Util._

import scala.reflect.ClassTag


object MyApp extends App {
    val conf = new SparkConf().setAppName("ExamWordcount")
    val sc = new SparkContext(conf)

    val wordcount = recmapreduce[String, Int](sc,_:Path,((s, path)=> s.split(" ").map((_,1))),(_+_))
    //val index = //a completer

    wordcount(new Path("/in"))

    //index(new Path("/in2")).saveAsTextFile("/out2")
}

package datacloud.spark.core.lastfm

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object HitParade {
  case class TrackId(id:String)
  case class UserId(id:String)
  
  def loadAndMergeDuplicates(sc: SparkContext ,url : String): RDD[((UserId, TrackId), (Int, Int, Int))] = {
    val textFile = sc.textFile(url)
    val rdd1 = textFile.map(_.split(" "))
    val rdd2 = rdd1.map(x => ((UserId(x(0)),TrackId(x(1))),(x(2).toInt,x(3).toInt,x(4).toInt)))
    val rdd3 = rdd2.reduceByKey((a,b) => (a._1+b._1, a._2+b._2, a._3+b._3))
    return rdd3
  }

  def hitparade(rdd:RDD[((UserId,TrackId),(Int,Int,Int))]): RDD[TrackId] = {
    val rdd1 = rdd.map(x => if(x._2._1+x._2._2 > 0){(x._1._2, (x._2._1, x._2._2, x._2._3, 1))} else{(x._1._2, (x._2._1, x._2._2, x._2._3, 0))})
    val rdd2 = rdd1.reduceByKey((a,b) => (a._1+b._1, a._2+b._2, a._3+b._3, a._4+b._4))
    val rdd3 = rdd2.map(x => (x._1, x._2._4, x._2._1+x._2._2-x._2._3))
    val rdd4 = rdd3.sortBy(x => ((-x._2, -x._3), x._1.id))
    val rdd5 = rdd4.map(_._1)
    return rdd5
  }
}
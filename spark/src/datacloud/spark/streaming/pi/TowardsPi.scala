package datacloud.spark.streaming.pi

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.log4j._

object TowardsPi extends App{
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Towards Pi").setMaster("local[2]")
    val ssc = new StreamingContext(new SparkContext(conf), Seconds(2))
    val lines = ssc.socketTextStream("localhost", 4242)
    ssc.checkpoint("/tmp/towardsPi")
    
    
    //Le flux actuel et les informations sur l'état précédent 
    //forment une nouvelle information sur l'état.
    //(Pourquoi Option, parce que Option contient Some et None, par exemple, lors de la première agrégation, il n'y a pas d'information sur l'état antérieur, seulement le flux actuel).
    val words = lines.map(_.split(" ")).map(x => (x(0).toDouble, x(1).toDouble)).map { x => if (x._1 * x._1 + x._2 * x._2 < 1) (1, 1) else (1, 0) }
    val res = words.updateStateByKey((newValues:Seq[Int], status:Option[(Int,Int)])=>{
      
        //traitement des nouvelles données
        val newRes = newValues.sum
        val newCount = newValues.size
        
        //Récupérer le dernier statut dans le message d'état.
        val lastStatus = status.getOrElse(None)
        lastStatus match {
          case None => Some((newRes,newCount))
          case last : (Int,Int) =>
            val lastRes = last._1
            val lastCount = last._2
            // Retourne un objet Option
            Some((lastRes + newRes, lastCount+newCount)) //Somme des valeurs actuelles et des valeurs historiques
        }
    })
    val result = res.map(x=> 4*x._2._1.toDouble/x._2._2)
    result.print()
    ssc.start()
    println("*** Towards Pi ***")
    ssc.awaitTermination()
}
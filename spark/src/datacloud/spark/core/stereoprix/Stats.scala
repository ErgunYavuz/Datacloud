package datacloud.spark.core.stereoprix

import org.apache.spark._ 
import org.apache.spark.rdd._
object Stats{
  
  
  def chiffreAffaire(url : String, annee : Int) :Int = {
    val conf = new SparkConf().setAppName("chiffreAffaire").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(url)
    val rdd1 = textFile.map(_.split(" "))
    val rdd2 = rdd1.map(x =>(x(0).split("_"),x(2).toInt))
    val rdd3 = rdd2.filter(x => x._1(2).toInt.equals(annee))
    val rdd4 = rdd3.map(x => x._2)
    val rdd5 = rdd4.reduce(_+_)
    sc.stop()
    println(rdd5)
    return rdd5
  }
  
  def chiffreAffaireParCategorie(url : String, urls:String) = {
    val conf = new SparkConf().setAppName("chiffreAffaire").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(url)
    val rdd1 = textFile.map(_.split(" "))
    val rdd2 = rdd1.map(x => (x(4), x(2).toInt))
    val rdd3 = rdd2.groupByKey()
    val rdd4 = rdd3.map(x => (x._1, x._2.sum))
    val rdd5 = rdd4.map(x=> x._1+":"+x._2) 
    rdd5.saveAsTextFile(urls)
    sc.stop()
  }
  
  //JJ_MM_AAAA_hh_mm magasin prix produit categorie
  def produitLePlusVenduParCategorie(url : String, urls:String) = {
    val conf = new SparkConf().setAppName("chiffreAffaire").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(url)
    val rdd1 = textFile.map(_.split(" "))
    val rdd2 = rdd1.map(x => ((x(4),x(3)),1))
    val rdd3 = rdd2.reduceByKey(_+_)
    val rdd4 = rdd3.map(x => (x._1._1, (x._1._2, x._2))).groupByKey().map(x => (x._1,x._2.maxBy(_._2)))
    val rdd5 = rdd4.map(x=> x._1+":"+x._2._1)
    rdd5.saveAsTextFile(urls)
    sc.stop()
  }

}
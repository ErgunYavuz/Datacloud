

package datacloud.spark.core.matrix

import org.apache.spark._
import org.apache.spark.rdd._

object MatrixIntAsRDD{
  implicit def rddToMat(rdd:RDD[VectorInt]): MatrixIntAsRDD = new MatrixIntAsRDD(rdd)
  
  def makeFromFile(url:String, nbpart:Int, sc:SparkContext): MatrixIntAsRDD = {
    val textFile = sc.textFile(url,nbpart)
    val rdd1 = textFile.map(_.split(" ")).map(_.map(_.toInt))
    val rdd2 = rdd1.map(new VectorInt(_))
    val rdd3 = rdd2.zipWithIndex().sortBy(_._2).map(_._1)
    return new MatrixIntAsRDD(rdd3)
  }
}

class MatrixIntAsRDD(val lines:RDD[VectorInt]){
  
  override def toString={
    val sb = new StringBuilder()
    lines.collect().foreach(line=> sb.append(line+"\n"))
    sb.toString()
  }
  
  def nbLines():Int = lines.count().toInt
  
  def nbColumns():Int = lines.first().length
  
  def get(i: Int, j: Int): Int = lines.zipWithIndex().filter(_._2 == i).first()._1.elements(j)

  override def equals(a:Any):Boolean = {
    a match {
      case tmp: MatrixIntAsRDD => 
        val rdd1 = tmp.lines.zipWithIndex().map(x => (x._2,x._1))
        val rdd2 = lines.zipWithIndex().map(x => (x._2,x._1))
        val rdd3 = rdd1.join(rdd2).map(_._2)
        val rdd4 = rdd3.filter(x=> x._1 == x._2)
        rdd4.count() == lines.count()
      case _ => false
    }
  }
  
  def +(other: MatrixIntAsRDD):MatrixIntAsRDD = {
    val rdd1 = lines.zipWithIndex().map(x => (x._2,x._1))
    val rdd2 = other.lines.zipWithIndex().map(x => (x._2,x._1))
    val rdd3 = rdd1.join(rdd2).sortByKey()
    val rdd4 = rdd3.map(x => x._2._1 +(x._2._2))
    new MatrixIntAsRDD(rdd4)
  }
  
  def transpose(): MatrixIntAsRDD = {
    val rdd1 = lines.flatMap(_.elements.zipWithIndex)
    val rdd2 = rdd1.map(x => (x._2, x._1))
    val rdd3 = rdd2.groupByKey().sortByKey().map(x => (x._1, x._2.toArray))
    val rdd4 = rdd3.map(x => new VectorInt(x._2))
    return new MatrixIntAsRDD(rdd4)
  }
  
  def *(other : MatrixIntAsRDD) : MatrixIntAsRDD = {
    if(this.nbColumns() != other.nbLines())
      throw new Exception("nbCol != nbLines")
    val rddtr = this.transpose()
    val rdd1 = rddtr.lines.zip(other.lines)
    val rdd2 = rdd1.map(x => (x._1.prodD(x._2)))
    val rdd3=rdd2.flatMap(x=>(x.zipWithIndex))
	  val rdd4=rdd3.map(x=>(x._2,x._1))
	  val rdd5=rdd4.sortBy(x=>(x._1), ascending=true)
	  val rdd6=rdd5.reduceByKey(_+_)
	  val rdd7=rdd6.map(x=>(x._2))
	  return new MatrixIntAsRDD(rdd7)
  }
  
}


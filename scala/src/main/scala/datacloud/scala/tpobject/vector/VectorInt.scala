package datacloud.scala.tpobject.vector

class VectorInt(val elements: Array[Int]) {

  def length: Int = elements.size

  def get(i: Int): Int = elements(i);

  def tostring():String = {
    "( " + elements.mkString(" ") + " )";
  }

  override def equals(a:Any):Boolean = {
    a match {
      case tmp: VectorInt =>
        val l = length
        l == tmp.length && {
          var i = 0
          while (i < l && elements(i) == tmp.elements(i)) i += 1
          i == l
        }
      case _ => false;
    }
  }


  def +(other:VectorInt): VectorInt = {
    var arr: Array[Int] = new Array[Int](other.length)
    for (i <- 0 to other.length-1)
      arr.update(i,elements.apply(i) + other.get(i))
    new VectorInt(arr)
  }

  def *(v:Int):VectorInt = {
    var arr: Array[Int] = new Array[Int](elements.length)
    for (i <- 0 to elements.length-1)
      arr.update(i, elements.apply(i)*v)
    new VectorInt(arr)
  }

  def prodD(other: VectorInt): Array[VectorInt] = {
    val res = new Array[VectorInt](other.length)
    for (i <- 0 to length - 1)
      res(i) = other * this.get(i)
    res
  }

}//fin classe VectorInt

object VectorInt {
  implicit def arrayIntToVectorInt(value: Array[Int]): VectorInt = new VectorInt(value);
}

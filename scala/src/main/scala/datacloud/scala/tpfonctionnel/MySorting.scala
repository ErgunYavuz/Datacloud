package datacloud.scala.tpfonctionnel
import scala.math.Ordering
object MySorting {
    def isSorted[A](tab : Array[A], f : (A,A)=>Boolean) : Boolean = {
        if(tab.size<=1) return true
            if(f(tab(0),tab(1)))
                isSorted(tab.drop(1),f)
            else false
    }
    def ascending[T](a:T, b:T)(implicit ord : Ordering[T]):Boolean = {
        ord.compare(a, b) < 0
    }

    def descending[T](a:T, b:T)(implicit ord : Ordering[T]) = {
        ord.compare(a, b) > 0
    }
}
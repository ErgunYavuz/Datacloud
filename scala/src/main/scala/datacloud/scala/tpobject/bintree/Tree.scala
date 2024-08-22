package datacloud.scala.tpobject.bintree


sealed abstract class Tree[+T]
case object EmptyTree extends Tree[Nothing]
case class Node[T](elem : T, left : Tree[T], right : Tree[T]) extends Tree[T]

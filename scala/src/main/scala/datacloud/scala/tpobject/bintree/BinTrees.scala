package datacloud.scala.tpobject.bintree

object BinTrees {
  def contains(tree: IntTree, el: Int): Boolean = {
    tree match {
      case EmptyIntTree => false
      case NodeInt(e, left, right) => if(el==e) true else contains(left, el) || contains(right, el)
    }
  }

  def size(tree: IntTree):Int = {
    tree match {
      case EmptyIntTree => 0
      case NodeInt(_, left, right) => 1 + size(left) + size(right)
    }
  }

  def insert(tree: IntTree, el: Int): IntTree = {
    if (contains(tree, el)) return tree
    tree match {
      case EmptyIntTree => NodeInt(el, EmptyIntTree, EmptyIntTree)
      case NodeInt(e, left, right) => if (size(right)>size(left)) NodeInt(e,insert(left,el),right) else NodeInt(e,left,insert(right,el))
    }
  }

  def contains[T](tree: Tree[T], el: T): Boolean = {
    tree match {
      case EmptyTree => false
      case Node(e, left, right) => if (el == e) true else contains(left, el) || contains(right, el)
    }
  }


  def size[T](tree: Tree[T]): Int = {
    tree match {
      case EmptyTree => 0
      case Node(_, left, right) => 1 + size(left) + size(right)
    }
  }

  def insert[T](tree: Tree[T], el: T): Tree[T] = {
    if (contains(tree, el)) return tree
    tree match {
      case EmptyTree => Node(el, EmptyTree, EmptyTree)
      case Node(e, left, right) => if (size(right)>size(left)) Node(e,insert(left,el),right) else Node(e,left,insert(right,el))
    }
  }
}

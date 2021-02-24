package datacloud.scala.tpobject.bintree

object BinTrees {
  def contains(tree : IntTree , v : Int) : Boolean = {
    tree match {
      case EmptyIntTree =>   false
      case NodeInt(e,l,r) => {
        if (e == v)  return true
        contains(l,v) || contains(r,v)
      }
      case _=> false
    }
  }

  def insert(tree: IntTree, v: Int): IntTree ={
    tree match {
      case EmptyIntTree => NodeInt(v,EmptyIntTree , EmptyIntTree)
      case NodeInt(e,l,r)  => {
        if (size(l) < size(r)) NodeInt(e,insert(l,v),r)
        else NodeInt(e,l,insert(r,v))
      }
    }
  }
  def size(tree: IntTree): Int = {
    tree match {
      case EmptyIntTree => 0
      case NodeInt(e,l,r) => size(l)+size(r)+1
    }
  }


  //tree
  def contains[A](tree : Tree[A] , v : A) : Boolean = {
    tree match {
      case EmptyTree =>  false
      case Node(e,l,r) => {
        if (e == v)  return true
        contains(l,v) || contains(r,v)
      }
      case _=> false
    }
  }

  def insert[A](tree: Tree[A], v: A): Tree[A] ={
    tree match {
      case EmptyTree => Node(v,EmptyTree , EmptyTree)
      case Node(e,l,r)  => {
        if (size(l) < size(r)) Node(e,insert(l,v),r)
        else Node(e,l,insert(r,v))
      }
    }
  }
  def size[A](tree: Tree[A]): Int = {
    tree match {
      case EmptyTree => 0
      case Node(e,l,r) => size(l)+size(r)+1
    }
  }


}

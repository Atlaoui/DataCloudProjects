package datacloud.scala.tpfonctionnel

object MySorting {
  //val ascending = (a:A,b:A) => if(a<b) true else false
  def isSorted[A](items: Array[A], f: (A, A) => Boolean) : Boolean = {
    items.sliding(2).forall{ case Array(x, y) => f(x, y) }
    //for(Array(first :A , second: A) <- items.grouped(2)) if(!f(first,second)) return false
   // true
  }

 def descending[T](a:T,b:T)(implicit o : Ordering[T]) : Boolean  = {
    o.compare(a,b) >= 0
}

  def ascending[T](a:T,b:T)(implicit o : Ordering[T]) : Boolean = {
    o.compare(a,b) < 0
  }
}

package datacloud.scala.tpfonctionnel

import scala.collection.mutable

object Premiers {
  def premiers(n: Int) : List[Int] = {
    var list =  List.range(2,n)
    var s : Set[Int] = Set(2)
    for (i <- List.range(2,n)) {
      list =list.filter(_ % i != 0)
      s = s ++ list.take(1)
      list =list.drop(1)
    }
    s.toList
  }
  /*
  https://medium.com/coding-with-clarity/functional-vs-iterative-prime-numbers-in-scala-7e22447146f0
  def premiers(end: Int): List[Int] = {
    val odds = Stream.from(3, 2).takeWhile(_ <= Math.sqrt(end).toInt)
    val composites = odds.flatMap(i => Stream.from(i * i, 2 * i).takeWhile(_ <= end))
    Stream.from(3, 2).takeWhile(_ <= end).diff(composites).toList
  }*/


  def premiersWithRec(n: Int): List[Int] = {
    def rec(i: Int, primes: List[Int]): List[Int] = {
      if (i >= n) primes else if (primes.forall(i % _ != 0)) rec(i + 1, i :: primes) else rec(i + 1, primes)
    }
    rec(2, List())
  }

}

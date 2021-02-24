package datacloud.scala.tpfonctionnel

object Statistics {
  def average(value: List[(Double, Int)]) : Double = {
    //value.map{ case (n1,n2) => n1*n2}.sum/value.reduce(_._2 + _._2)
    value.map{ case (n1,n2) => n1*n2}.sum/value.map{ case (n1,n2) => n2}.reduce(_ + _)

  }
}

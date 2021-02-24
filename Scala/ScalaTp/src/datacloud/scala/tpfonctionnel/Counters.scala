package datacloud.scala.tpfonctionnel

object Counters {
  def nbLetters(l: List[String]) : Int = {
    //l.map(_.replace(" ","")).map { case (key) => key.length }.reduce({ (a, b) => a + b })
    //l.map(_.replace(" ", "")).map { case (key) => key.length }.sum
    l.flatMap(_.replace(" ","")).length
  }
}

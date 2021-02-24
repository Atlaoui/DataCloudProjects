package datacloud.scala.tpfonctionnel



object FunctionParty {
  def curryfie[A,B,C](f: (A, B) => C): A => B => C = {
    (a:A) => (b:B) => f(a,b)
  }
  def decurryfie[A,B,C](f: A => B => C):(A, B) => C ={
    (a:A,b:B) => f(a)(b)
  }
  def compose[A,B,C](f: B => C, g: A => B): A => C = {
    (a:A) => f(g(a))
  }
  def axplusb(a:Int,b:Int):Int => Int ={
   // (c:Int) => (a*c+b)
    val fois = curryfie((x:Int, y:Int) => x * y)
    val plus = curryfie((x:Int, y:Int) => x + y)
    (c:Int) => compose(plus(b),fois(a))(c)
  }
}

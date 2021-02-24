package datacloud.scala.tpobject.vector



class VectorInt (val elements :Array[Int] ) extends Serializable {

  def length(): Int = {
        elements.length
    }

    def get(i:Int) : Int = {
        elements(i)

    }

  override def toString() : String = {
        val s =  new StringBuilder("( ")
        for(elem <- elements ){
            s.append(elem.toString).append(" ")
        }
        s.append(" )")
        s.toString
    }
  def canEqual(a: Any) = a.isInstanceOf[VectorInt]

    override def equals(a:Any):Boolean = {
      a match {
        case a: VectorInt => {
          a.canEqual(this) && a.elements.sameElements(elements)
        }
        case _ => false
      }
    }

  def + (other:VectorInt):VectorInt = {
    val line = scala.collection.mutable.ListBuffer[Int]()
    for (i <- elements.indices){
      line+=(this.get(i)+other.get(i))
    }

    new VectorInt(line.toArray)
  }

  def *(v:Int):VectorInt = {
    val line = scala.collection.mutable.ListBuffer[Int]()
    for(elem <- elements ){
      line+=(elem*v)
    }
    new VectorInt(line.toArray)
  }



  def  prodD(other:VectorInt):Array[VectorInt] = {
    val ret = new Array[VectorInt](other.length())
      for (i <-elements.indices){
        val line = scala.collection.mutable.ListBuffer[Int]()
        for (j <- elements.indices){
            line+=(this.get(i)*other.get(j))
        }
        ret.update(i,new VectorInt(line.toArray))
      }
    ret
  }
}
object VectorInt {
  implicit def ArrayToVec(l: Array[Int])= new VectorInt(l)
  def apply(id: Array[Int]): VectorInt = {
				var t = new VectorInt(id)
						t
		}
}

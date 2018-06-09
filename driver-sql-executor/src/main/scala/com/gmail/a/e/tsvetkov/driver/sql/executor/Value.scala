package com.gmail.a.e.tsvetkov.driver.sql.executor

import scala.reflect.{ClassTag, classTag}

sealed trait Value {
  type RuntimeType

  def getValue[T <: Value](implicit t: ClassTag[T]): T#RuntimeType = {
    assert(classTag[T].runtimeClass == getClass)
    asInstanceOf[T].v
  }

  protected def v: RuntimeType
}

object Value {
  private def create[C <: Value](c: C#RuntimeType => C, v: Any) = {
    c(v.asInstanceOf[C#RuntimeType])
  }

  val map: Map[Class[_], Any => Value] = Map(
    (BooleanValue.getClass, create[BooleanValue](BooleanValue, _)),
    (StringValue.getClass, create[StringValue](StringValue, _)),
    (NumericValue.getClass, create[NumericValue](NumericValue, _))

  )

  def apply[T <: Value : ClassTag](v: Any): T = {
    map(classTag[T].runtimeClass )(v).asInstanceOf[T]
  }
}

case class BooleanValue(v: Boolean) extends Value {
  override type RuntimeType = Boolean
}

case class StringValue(v: String) extends Value {
  override type RuntimeType = String
}

case class NumericValue(v: BigDecimal) extends Value {
  override type RuntimeType = BigDecimal
}

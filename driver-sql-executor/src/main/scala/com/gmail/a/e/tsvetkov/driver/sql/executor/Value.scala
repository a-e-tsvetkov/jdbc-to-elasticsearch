package com.gmail.a.e.tsvetkov.driver.sql.executor

import scala.reflect.{ClassTag, classTag}

sealed trait Value {
  type RuntimeType

  def getValue[T <: Value](implicit t: ClassTag[T]): T#RuntimeType = {
    assert(t.runtimeClass == getClass,
      s"Type mismatch. Expected ${getClass}, but actual ${t.runtimeClass}")
    asInstanceOf[T].v
  }

  protected def v: RuntimeType
}

object Value {
  private def create[C <: Value](c: C#RuntimeType => C, v: Any) = {
    c(v.asInstanceOf[C#RuntimeType])
  }

  val map: Map[ClassTag[_], Any => Value] = Map(
    (classTag[BooleanValue], create[BooleanValue](BooleanValue, _)),
    (classTag[StringValue], create[StringValue](StringValue, _)),
    (classTag[NumericValue], create[NumericValue](NumericValue, _))

  )

  def apply[T <: Value : ClassTag](v: Any): T = {
    map(classTag[T])(v).asInstanceOf[T]
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

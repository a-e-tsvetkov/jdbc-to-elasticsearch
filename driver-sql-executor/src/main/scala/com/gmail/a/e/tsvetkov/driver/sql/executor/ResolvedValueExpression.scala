package com.gmail.a.e.tsvetkov.driver.sql.executor

import scala.reflect._

sealed trait ResolvedValueExpression {
  val valueType: ValueType
}

case class ResolvedValueExpressionConst
(
  valueType: ValueType,
  value: Value
) extends ResolvedValueExpression

case class ResolvedValueExpressionColumnRef
(
  table: ScopeTable,
  column: ScopeColumn
) extends ResolvedValueExpression {
  override val valueType: ValueType = column.valueType
}

case class ResolvedValueExpression1
(
  valueType: ValueType,
  operation: Op1,
  sub: ResolvedValueExpression
) extends ResolvedValueExpression

case class ResolvedValueExpression2
(
  valueType: ValueType,
  operation: Op2,
  left: ResolvedValueExpression,
  right: ResolvedValueExpression
) extends ResolvedValueExpression

sealed trait Op

sealed trait Op1 extends Op {
  def compute(subVal: Value): Value
}

case object OpBooleanNot extends Op1 {
  override def compute(v: Value): Value = {
    BooleanValue(!v.getValue[BooleanValue])
  }
}

case object OpUnaryMinus extends Op1 {
  override def compute(v: Value): Value = {
    NumericValue(-v.getValue[NumericValue])
  }
}

sealed trait Op2 extends Op {
  type Type <: Value
  type ResultType <: Value
  implicit val typeTag: ClassTag[Type]

  def compute
  (
    leftVal: Value,
    rightVal: Value
  ): ResultType
}

trait Op2Same extends Op2 {

  override type ResultType = Type

  def op
  (
    l: Type#RuntimeType,
    r: Type#RuntimeType
  ): Type#RuntimeType

  def compute
  (
    leftVal: Value,
    rightVal: Value
  ): ResultType = {
    val l = leftVal.getValue[Type]
    val r = rightVal.getValue[Type]
    val rr = op(l, r)
    Value.apply[ResultType](rr)
  }

}

trait Op2Compare extends Op2 {
  override type Type = Value
  override type ResultType = BooleanValue
  override val typeTag: ClassTag[Value] = throw new RuntimeException("Not supported")

  def opString(l: String, r: String): Boolean

  def opBoolean(l: Boolean, r: Boolean): Boolean

  def opNumeric(l: BigDecimal, r: BigDecimal): Boolean

  def compute
  (
    leftVal: Value,
    rightVal: Value
  ): BooleanValue = {
    def conv[T <: Value : ClassTag] = (leftVal.getValue[T], leftVal.getValue[T])

    Value.apply[BooleanValue](leftVal match {
      case BooleanValue(_) =>
        val (l, r) = conv[BooleanValue]
        opBoolean(l, r)
      case StringValue(_) =>
        val (l, r) = conv[StringValue]
        opString(l, r)
      case NumericValue(_) =>
        val (l, r) = conv[NumericValue]
        opNumeric(l, r)
    })
  }
}

trait OpBoolean extends Op2Same {
  override type Type = BooleanValue

  override val typeTag: ClassTag[Type] = classTag[BooleanValue]
}

case object OpBooleanOr extends Op2 with OpBoolean {
  override def op(l: Boolean, r: Boolean): Boolean = l | r
}

case object OpBooleanAnd extends Op2 with OpBoolean {
  override def op(l: Boolean, r: Boolean): Boolean = l & r
}

case object OpCompareEq extends Op2 with Op2Compare {
  override def opString(l: String, r: String): Boolean = l == r

  override def opBoolean(l: Boolean, r: Boolean): Boolean = l == r

  override def opNumeric(l: BigDecimal, r: BigDecimal): Boolean = l == r
}

case object OpCompareNe extends Op2 with Op2Compare {
  override def opString(l: String, r: String): Boolean = l != r

  override def opBoolean(l: Boolean, r: Boolean): Boolean = l != r

  override def opNumeric(l: BigDecimal, r: BigDecimal): Boolean = l != r
}

case object OpCompareGt extends Op2 with Op2Compare {
  override def opString(l: String, r: String): Boolean = l > r

  override def opBoolean(l: Boolean, r: Boolean): Boolean = l > r

  override def opNumeric(l: BigDecimal, r: BigDecimal): Boolean = l > r
}

case object OpCompareLt extends Op2 with Op2Compare {
  override def opString(l: String, r: String): Boolean = l < r

  override def opBoolean(l: Boolean, r: Boolean): Boolean = l < r

  override def opNumeric(l: BigDecimal, r: BigDecimal): Boolean = l < r
}

case object OpCompareGe extends Op2 with Op2Compare {
  override def opString(l: String, r: String): Boolean = l >= r

  override def opBoolean(l: Boolean, r: Boolean): Boolean = l >= r

  override def opNumeric(l: BigDecimal, r: BigDecimal): Boolean = l >= r
}

case object OpCompareLe extends Op2 with Op2Compare {
  override def opString(l: String, r: String): Boolean = l <= r

  override def opBoolean(l: Boolean, r: Boolean): Boolean = l <= r

  override def opNumeric(l: BigDecimal, r: BigDecimal): Boolean = l <= r
}

trait OpNumeric extends Op2Same {
  override type Type = NumericValue
  override val typeTag: ClassTag[Type] = classTag[NumericValue]
}

case object OpNumericPlus extends Op2 with OpNumeric {
  override type Type = NumericValue

  override def op(l: BigDecimal, r: BigDecimal): BigDecimal = l + r
}

case object OpNumericMinus extends Op2 with OpNumeric {
  override type Type = NumericValue

  override def op(l: BigDecimal, r: BigDecimal): BigDecimal = l - r
}

case object OpNumericMult extends Op2 with OpNumeric {
  override type Type = NumericValue

  override def op(l: BigDecimal, r: BigDecimal): BigDecimal = l * r
}

case object OpNumericDiv extends Op2 with OpNumeric {
  override type Type = NumericValue

  override def op(l: BigDecimal, r: BigDecimal): BigDecimal = l / r
}

case object OpStringConcat extends Op2 with Op2Same {
  override type Type = StringValue

  override val typeTag: ClassTag[StringValue] = classTag[StringValue]

  override def op(l: String, r: String): String = l + r
}
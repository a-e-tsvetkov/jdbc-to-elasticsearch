package com.gmail.a.e.tsvetkov.driver.sql.executor

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

sealed trait Op1 extends Op

case object OpBooleanNot extends Op1

case object OpUnaryMinus extends Op1

sealed trait Op2 extends Op

case object OpBooleanOr extends Op2

case object OpBooleanAnd extends Op2

case object OpCompareEq extends Op2

case object OpCompareNe extends Op2

case object OpCompareGt extends Op2

case object OpCompareLt extends Op2

case object OpCompareGe extends Op2

case object OpCompareLe extends Op2

case object OpNumericPlus extends Op2

case object OpNumericMinus extends Op2

case object OpNumericMult extends Op2

case object OpNumericDiv extends Op2

case object OpStringConcat extends Op2
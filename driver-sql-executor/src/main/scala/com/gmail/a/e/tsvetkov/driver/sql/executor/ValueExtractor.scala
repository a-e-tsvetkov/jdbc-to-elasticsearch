package com.gmail.a.e.tsvetkov.driver.sql.executor

trait ValueExtractor {

  def extractValue(fields: Map[String, AnyRef], column: ScopeColumn): Value = {
    column.valueType match {
      case MetadataTypeChar =>
        StringValue(fields(column.name).asInstanceOf[String])
      case MetadataTypeNumeric =>
        NumericValue(
          BigDecimal(fields(column.name).asInstanceOf[String]))
      case MetadataTypeBoolean =>
        BooleanValue(fields(column.name).asInstanceOf[Boolean])
    }
  }

  def evaluateExpression(fields: Map[ScopeColumn, Value])(expr: ResolvedValueExpression): Value =
    expr match {
      case ResolvedValueExpressionConst(valueType, value) => value
      case ResolvedValueExpressionColumnRef(column) => fields(column)
      case ResolvedValueExpression1(_, op, sub) =>
        val subVal = evaluateExpression(fields)(sub)
        op.compute(subVal)
      case ResolvedValueExpression2(valueType, op, left, right) =>
        val leftVal = evaluateExpression(fields)(left)
        val rightVal = evaluateExpression(fields)(right)
        op.compute(leftVal, rightVal)
    }

}

package com.gmail.a.e.tsvetkov.driver.sql.executor

trait ValueExtractor {

private  def extractValue(fields: Map[String, AnyRef], column: ScopeColumn): Value = {
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

  def extract(fields: Map[String, AnyRef])(expr: ResolvedValueExpression): Value =
    expr match {
      case ResolvedValueExpressionConst(valueType, value) => value
      case ResolvedValueExpressionColumnRef(table, column) => extractValue(fields, column)
      case ResolvedValueExpression1(_, op, sub) =>
        val subVal = extract(fields)(sub)
        op.compute(subVal)
      case ResolvedValueExpression2(valueType, op, left, right) =>
        val leftVal = extract(fields)(left)
        val rightVal = extract(fields)(right)
        op.compute(leftVal, rightVal)
    }

}

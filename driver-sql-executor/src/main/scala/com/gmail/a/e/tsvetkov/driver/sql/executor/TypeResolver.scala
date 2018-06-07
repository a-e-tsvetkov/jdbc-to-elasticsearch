package com.gmail.a.e.tsvetkov.driver.sql.executor

import com.gmail.a.e.tsvetkov.driver.sql._
import com.gmail.a.e.tsvetkov.driver.sql.executor.Util.err

case class TypeResolver(scope: Scope) {

  def resolve(expression: ValueExpression): ResolvedValueExpression = {
    expression match {
      case b: BooleanExpression => resolveBoleanExpression(b)
      case n: NumericExpression => resolveNumericExpression(n)
      case s: StringExpression => resolveStringExpression(s)
      case ValueExpressionColumnReference(id) => resolveReferenceExpression(id)
    }
  }

  private def resolveReferenceExpression(id: SqlIdentifier) = {
    id.terms match {
      case Seq(tableName, columnName) =>
        val t = scope.tables
          .find(t => t.alias == tableName)
          .getOrElse(err("table not found"))
        val c = t.columns.find(c => c.name == columnName)
          .getOrElse(err("column not found"))
        ResolvedValueExpressionColumnRef(t, c)
      case Seq(columnName) =>
        val (t, c) = scope.tables
          .flatMap(t => t.columns.map((t, _)))
          .find { case (_, column) => column.name == columnName }
          .getOrElse(err("column not found"))
        ResolvedValueExpressionColumnRef(t, c)
      case _ => err("invalid reference " + id)
    }
  }

  private def resolveStringExpression(expression: StringExpression) = {
    expression match {
      case StringExpressionBinary(operation, left, right) =>
        val sub1 = resolve(left)
        val sub2 = resolve(right)
        assertType(sub1, MetadataTypeChar)
        assertType(sub2, MetadataTypeChar)
        ResolvedValueExpression2(
          MetadataTypeChar,
          mapOperation(operation),
          sub1,
          sub2)
    }
  }

  private def resolveNumericExpression(expression: NumericExpression) = {
    expression match {
      case NumericExpressionBinary(operation, left, right) =>
        val sub1 = resolve(left)
        val sub2 = resolve(right)
        assertType(sub1, MetadataTypeNumeric)
        assertType(sub2, MetadataTypeNumeric)
        ResolvedValueExpression2(
          MetadataTypeNumeric,
          mapOperation(operation),
          sub1,
          sub2)
      case NumericExpressionUnaryMinus(value) =>
        val sub = resolve(value)
        assertType(sub, MetadataTypeNumeric)
        ResolvedValueExpression1(
          MetadataTypeNumeric,
          OpUnaryMinus,
          sub)
      case NumericExpressionConstant(value) =>
        ResolvedValueExpressionConst(
          MetadataTypeNumeric,
          NumericValue(BigDecimal(value)))
    }
  }

  private def resolveBoleanExpression(expression: BooleanExpression) = {
    expression match {
      case BooleanExpressionBinary(operation, left, right) =>
        val sub1 = resolve(left)
        val sub2 = resolve(right)
        assertType(sub1, MetadataTypeBoolean)
        assertType(sub2, MetadataTypeBoolean)
        ResolvedValueExpression2(
          MetadataTypeBoolean,
          mapOperation(operation),
          sub1,
          sub2)
      case BooleanExpressionNot(expr) =>
        val sub = resolve(expr)
        assertType(sub, MetadataTypeBoolean)
        ResolvedValueExpression1(MetadataTypeBoolean, OpBooleanNot, sub)
      case BooleanExpressionComparision(operation, left, right) =>
        val sub1 = resolve(left)
        val sub2 = resolve(right)
        assertTypeEqual(sub1.valueType, sub2.valueType)
        ResolvedValueExpression2(
          MetadataTypeBoolean,
          mapOperation(operation),
          sub1,
          sub2)
    }
  }

  private def assertType(expr: ResolvedValueExpression, valueType: ValueType) = {
    assertTypeEqual(expr.valueType, valueType)
  }

  private def assertTypeEqual(t1: ValueType, t2: ValueType) = {
    if (t2 != t1) {
      err("Incompatible types")
    }
  }

  private def mapOperation(op: BooleanOperarion) = {
    op match {
      case BooleanOperarionOr => OpBooleanOr
      case BooleanOperarionAnd => OpBooleanAnd
    }
  }

  private def mapOperation(op: ComparisionOperarion) = {
    op match {
      case ComparisionOperarionEq => OpCompareEq
      case ComparisionOperarionNe => OpCompareNe
      case ComparisionOperarionGt => OpCompareGt
      case ComparisionOperarionLt => OpCompareLt
      case ComparisionOperarionGe => OpCompareGe
      case ComparisionOperarionLe => OpCompareLe
    }
  }

  private def mapOperation(op: NumericOperarion) = {
    op match {
      case NumericOperarionPlus => OpNumericPlus
      case NumericOperarionMinus => OpNumericMinus
      case NumericOperarionMult => OpNumericMult
      case NumericOperarionDiv => OpNumericDiv
    }
  }

  private def mapOperation(op: StringOperarion) = {
    op match {
      case StringOperarionConcat => OpStringConcat
    }
  }
}

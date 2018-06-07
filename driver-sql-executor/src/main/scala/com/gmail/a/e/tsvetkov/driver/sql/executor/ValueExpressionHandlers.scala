package com.gmail.a.e.tsvetkov.driver.sql.executor
import com.gmail.a.e.tsvetkov.driver.sql._
import com.gmail.a.e.tsvetkov.driver.sql.executor.Util.err

trait ValueExpressionHandlers {

  private def asBoolean(value: Value) = {
    value match {
      case v: BooleanValue => v
      case v: StringValue => err("Is not boolean: " + v.v)
      case v: NumericValue => err("Is not boolean: " + v.v)
    }
  }

  private def asNumeric(value: Value) = {
    value match {
      case v: BooleanValue => err("Is not numeric: " + v.v)
      case v: StringValue => err("Is not numeric: " + v.v)
      case v: NumericValue => v
    }
  }

  private def asString(value: Value) = {
    value match {
      case v: BooleanValue => err("Is not string: " + v.v)
      case v: StringValue => v
      case v: NumericValue => err("Is not string: " + v.v)
    }
  }

  private def extractValue(v: BooleanExpression): BooleanValue = {
    BooleanValue(v match {
      case BooleanExpressionBinary(operation, le, re) =>
        var l = asBoolean(extractValue(le))
        var r = asBoolean(extractValue(re))
        operation match {
          case BooleanOperarionOr => l.v | r.v
          case BooleanOperarionAnd => l.v & r.v
        }
      case BooleanExpressionNot(e) =>
        var v = asBoolean(extractValue(e))
        !v.v
      case BooleanExpressionComparision(operation, le, re) =>
        var l = extractValue(le)
        var r = extractValue(re)
        l.asInstanceOf[BooleanValue]
          .compare(
            operation,
            l.asInstanceOf[BooleanValue],
            r.asInstanceOf[BooleanValue])
    })
  }

  private def extractValue(v: NumericExpression): NumericValue = {
    NumericValue(v match {
      case NumericExpressionBinary(operation, le, re) =>
        var l = asNumeric(extractValue(le))
        var r = asNumeric(extractValue(re))
        operation match {
          case NumericOperarionPlus => l.v + r.v
          case NumericOperarionMinus => l.v - r.v
          case NumericOperarionMult => l.v * r.v
          case NumericOperarionDiv => l.v / r.v
        }
      case NumericExpressionUnaryMinus(ve) =>
        var v = asNumeric(extractValue(ve))
        -v.v
      case NumericExpressionConstant(value) => BigDecimal(value)
    })
  }

  private def extractValue(v: StringExpression): StringValue = {
    StringValue(v match {
      case StringExpressionBinary(operation, le, re) =>
        val l = asString(extractValue(le))
        val r = asString(extractValue(re))
        operation match {
          case StringOperarionConcat => l.v + r.v
        }
    })
  }

  def extractValue(v: ValueExpression): Value = {
    v match {
      case b: BooleanExpression => extractValue(b)
      case n: NumericExpression => extractValue(n)
      case s: StringExpression => extractValue(s)
      case b: ValueExpressionBinary => extractValue(b)
      case ValueExpressionColumnReference(id) => err("No column reference allowed")
    }
  }

  def toJsonValue(value: Value) = {
    value match {
      case BooleanValue(v) => v
      case StringValue(v) => v
      case NumericValue(v) => v.toString
    }
  }
}

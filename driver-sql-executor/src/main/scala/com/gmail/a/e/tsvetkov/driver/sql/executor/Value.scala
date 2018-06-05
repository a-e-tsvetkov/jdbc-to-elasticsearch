package com.gmail.a.e.tsvetkov.driver.sql.executor

import com.gmail.a.e.tsvetkov.driver.sql._
import com.gmail.a.e.tsvetkov.driver.sql.executor.Util.err

sealed trait ValueType {
  val comparator: (ComparisionOperarion, Value, Value) => Boolean
}


sealed trait Value {
  val valueType: ValueType
}

case class BooleanValue(v: Boolean) extends Value {

  case object BooleanValueType extends ValueType {
    override val comparator = (op: ComparisionOperarion, l: Value, r: Value) =>
      compare(op, l.asInstanceOf[BooleanValue], r.asInstanceOf[BooleanValue])
  }

  def compare(op: ComparisionOperarion, l: BooleanValue, r: BooleanValue) = {
    op match {
      case ComparisionOperarionEq => l.v == r.v
      case ComparisionOperarionNe => l.v != r.v
      case ComparisionOperarionGt => err("Not supported for boolean")
      case ComparisionOperarionLt => err("Not supported for boolean")
      case ComparisionOperarionGe => err("Not supported for boolean")
      case ComparisionOperarionLe => err("Not supported for boolean")
    }
  }

  override val valueType = BooleanValueType
}

case class StringValue(v: String) extends Value {

  case object StringValueType extends ValueType {
    override val comparator = (op: ComparisionOperarion, l: Value, r: Value) =>
      compare(op, l.asInstanceOf[StringValue], r.asInstanceOf[StringValue])
  }

  def compare(op: ComparisionOperarion, l: StringValue, r: StringValue) = {
    op match {
      case ComparisionOperarionEq => l.v == r.v
      case ComparisionOperarionNe => l.v != r.v
      case ComparisionOperarionGt => l.v > r.v
      case ComparisionOperarionLt => l.v < r.v
      case ComparisionOperarionGe => l.v >= r.v
      case ComparisionOperarionLe => l.v <= r.v
    }
  }


  override val valueType = StringValueType
}

case class NumericValue(v: BigDecimal) extends Value {

  case object NumericValueType extends ValueType {
    override val comparator = (op: ComparisionOperarion, l: Value, r: Value) =>
      compare(op, l.asInstanceOf[NumericValue], r.asInstanceOf[NumericValue])
  }

  def compare(op: ComparisionOperarion, l: NumericValue, r: NumericValue) = {
    op match {
      case ComparisionOperarionEq => l.v == r.v
      case ComparisionOperarionNe => l.v != r.v
      case ComparisionOperarionGt => l.v > r.v
      case ComparisionOperarionLt => l.v < r.v
      case ComparisionOperarionGe => l.v >= r.v
      case ComparisionOperarionLe => l.v <= r.v
    }
  }

  override val valueType = NumericValueType
}

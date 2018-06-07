package com.gmail.a.e.tsvetkov.driver.sql.executor

import com.gmail.a.e.tsvetkov.driver.sql._
import com.gmail.a.e.tsvetkov.driver.sql.executor.Util.err

sealed trait Value

case class BooleanValue(v: Boolean) extends Value {
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
}

case class StringValue(v: String) extends Value {
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
}

case class NumericValue(v: BigDecimal) extends Value {
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
}

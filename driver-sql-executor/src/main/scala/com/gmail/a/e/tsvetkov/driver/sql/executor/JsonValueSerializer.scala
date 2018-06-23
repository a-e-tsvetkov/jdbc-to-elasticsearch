package com.gmail.a.e.tsvetkov.driver.sql.executor

trait JsonValueSerializer {
  def toJsonValue(value: Value) = {
    value match {
      case BooleanValue(v) => v
      case StringValue(v) => v
      case NumericValue(v) => v.toString
    }
  }

}

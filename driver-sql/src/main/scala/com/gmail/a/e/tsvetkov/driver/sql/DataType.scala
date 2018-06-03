package com.gmail.a.e.tsvetkov.driver.sql

sealed trait DataType

case class DataTypeChar(length: Option[Int]) extends DataType
case class DataTypeNumeric(spec: Option[DataTypeNumericSpec]) extends DataType
case class DataTypeFloat(precission: Option[Int]) extends DataType
object DataTypeReal extends DataType
object DataTypeDouble extends DataType

case class DataTypeNumericSpec(precission: Int, scale: Option[Int])
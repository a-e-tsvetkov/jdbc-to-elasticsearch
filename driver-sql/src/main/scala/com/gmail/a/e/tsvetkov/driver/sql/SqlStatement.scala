package com.gmail.a.e.tsvetkov.driver.sql

sealed trait SqlStatement
object SqlBlahStatement extends SqlStatement

case class SqlCreateTableStatement(name: String, columnDefenition: Seq[ColumnDefenition]) extends SqlStatement

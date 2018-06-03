package com.gmail.a.e.tsvetkov.driver.sql.parser

import com.gmail.a.e.tsvetkov.driver.sql.SqlStatement

sealed trait SqlParseResult{
  def isError : Boolean

  def statement: SqlStatement

  def error: String

}

case class SqlParseResultSuccess(statement: SqlStatement) extends SqlParseResult {
  override def isError: Boolean = false

  override def error: String = throw new RuntimeException
}

case class SqlParseResultFailure(error: String) extends SqlParseResult {
  override def isError: Boolean = true

  override def statement: SqlStatement = throw new RuntimeException
}

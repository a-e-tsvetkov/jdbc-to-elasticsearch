package com.gmail.a.e.tsvetkov.driver.sql.executor

import com.gmail.a.e.tsvetkov.driver.sql.{SqlBlahStatement, SqlCreateTableStatement, SqlStatement}

object SqlExecutor {
  def execute(statement: SqlStatement): Unit = {
    statement match {
      case SqlBlahStatement => ???

      case SqlCreateTableStatement(name, columnDefenition) =>
    }
  }
}

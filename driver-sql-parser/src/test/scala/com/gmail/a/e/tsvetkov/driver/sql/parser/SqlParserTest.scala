package com.gmail.a.e.tsvetkov.driver.sql.parser

import com.gmail.a.e.tsvetkov.driver.sql.{DataTypeNumeric, SqlBlahStatement, SqlCreateTableStatement}
import org.scalatest.FunSuite
import org.scalatest.Assertions._

class SqlParserTest extends FunSuite {

  test("parse blah statement") {
    val result = SqlParser.parse("blah")
    assert(result.isInstanceOf[SqlParseResultSuccess])
    assert(result.asInstanceOf[SqlParseResultSuccess].statement == SqlBlahStatement)
  }

  test("parse simple create table statement") {
    val result = SqlParser.parse("create table t1 (f1 int)")
    assert(result.isInstanceOf[SqlParseResultSuccess])
    val statement = result.asInstanceOf[SqlParseResultSuccess].statement
    assert(statement.isInstanceOf[SqlCreateTableStatement])
    statement match {
      case SqlCreateTableStatement(name, cd) =>
        assert(name == "t1")
        assert(cd.length == 1)
        assert(cd.head.name == "f1")
        assert(cd.head.dataType.isDefined)
        val dataType = cd.head.dataType.head
        assert(dataType.isInstanceOf[DataTypeNumeric])
        assert(dataType.asInstanceOf[DataTypeNumeric].spec.isEmpty)
    }
  }
}

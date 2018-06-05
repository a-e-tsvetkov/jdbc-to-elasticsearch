package com.gmail.a.e.tsvetkov.driver.sql.parser

import com.gmail.a.e.tsvetkov.driver.sql._
import org.scalatest.FunSuite

class SqlParserTest extends FunSuite {

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

  test("parse simple select statement") {
    val result = SqlParser.parse("select f1 f2 from t1")
    assert(result.isInstanceOf[SqlParseResultSuccess])
    val statement = result.asInstanceOf[SqlParseResultSuccess].statement
    assert(statement.isInstanceOf[SqlSelectStatement])
    statement match {
      case SqlSelectStatement(terms, from) =>
        assert(terms.length == 1)
        assert(terms(0).isInstanceOf[SelectTermExpr])
        terms(0) match {
          case SelectTermExpr(expr, label) =>
            assert(label.isDefined)
            assert(label.get == "f2")
            assert(expr.isInstanceOf[ValueExpressionColumnReference])
            assert(expr.asInstanceOf[ValueExpressionColumnReference].id == Seq("f1"))
        }
    }
  }

  test("parse simple insert statement") {
    val result = SqlParser.parse("insert into t1(f1, f2) values ( 1, 2)")
    assert(result.isInstanceOf[SqlParseResultSuccess])
    val statement = result.asInstanceOf[SqlParseResultSuccess].statement
    assert(statement.isInstanceOf[SqlInsertStatement])
    statement match {
      case SqlInsertStatement(t, c, s) =>
        assert(t == "t1")
        assert(c.isDefined)
        assert(c.get == Seq("f1", "f2"))
        assert(s == Seq(
          Seq(
            NumericExpressionConstant("1"),
            NumericExpressionConstant("2"))))
    }
  }
}

package com.gmail.a.e.tsvetkov.driver.sql.parser

import com.gmail.a.e.tsvetkov.driver.sql._
import org.scalatest.FunSuite

import scala.reflect.{ClassTag, classTag}

class SqlParserTest extends FunSuite {

  test("parse simple create table statement") {
    val result = SqlParser.parse("create table t1 (f1 int)")
    val statement = assertResult[SqlCreateTableStatement](result)
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
    val result = SqlParser.parse("select f1 as ff1, f2 ff2 from t1, t2 a2")
    val statement = assertResult[SqlSelectStatement](result)
    assert(statement.from == Seq(
      TableReferencePrimary("t1", None),
      TableReferencePrimary("t2", Some("a2"))
    ))
    assert(statement.terms == Seq(
      SelectTermExpr(
        ValueExpressionColumnReference(SqlIdentifier(Seq("f1"))),
        Some("ff1")),
      SelectTermExpr(
        ValueExpressionColumnReference(SqlIdentifier(Seq("f2"))),
        Some("ff2"))))

  }

  test("parse select statement with expressions") {
    val result = SqlParser.parse("select 1 + 1, 1 - f1, 2 = f3 from t1")
    val statement = assertResult[SqlSelectStatement](result)
    assert(statement.terms == Seq(
      SelectTermExpr(
        NumericExpressionBinary(
          NumericOperarionPlus,
          NumericExpressionConstant("1"),
          NumericExpressionConstant("1")),
        None),
      SelectTermExpr(
        NumericExpressionBinary(
          NumericOperarionMinus,
          NumericExpressionConstant("1"),
          ValueExpressionColumnReference(
            SqlIdentifier(List("f1")))),
        None),
      SelectTermExpr(
        BooleanExpressionComparision(
          ComparisionOperarionEq,
          NumericExpressionConstant("2"),
          ValueExpressionColumnReference(
            SqlIdentifier(List("f3")))),
        None)))
  }

  test("parse simple insert statement") {
    val result = SqlParser.parse("insert into t1(f1, f2) values ( 1, 2)")
    val statement = assertResult[SqlInsertStatement](result)
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


  test("parse simple where in select statement") {
    val result = SqlParser.parse("select a from b where x=y")
    val statement = assertResult[SqlSelectStatement](result)

    assert(statement.where.get ==
      BooleanExpressionComparision(
        ComparisionOperarionEq,
        ValueExpressionColumnReference(
          SqlIdentifier(List("x"))),
        ValueExpressionColumnReference(
          SqlIdentifier(List("y")))))
  }

  private def assertResult[T: ClassTag](result: SqlParseResult) = {
    assert(result.isInstanceOf[SqlParseResultSuccess])
    val statement = result.asInstanceOf[SqlParseResultSuccess].statement
    assert(classTag[T].runtimeClass == statement.getClass)
    statement.asInstanceOf[T]
  }
}

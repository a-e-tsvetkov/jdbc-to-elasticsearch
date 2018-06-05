package com.gmail.a.e.tsvetkov.driver.sql.parser

import com.gmail.a.e.tsvetkov.driver.sql.parser.SqlToken._
import org.scalatest.FunSuite

import scala.util.parsing.input.Reader

class SqlLexerIntTest extends FunSuite {

  test("parse identifier") {
    val tokens = SqlLexerInt.parse("INSERT into")
    assert(toSeq(tokens) ==
      Seq(INSERT, INTO))
  }

  test("parse delimitedIdentifier") {
    val tokens = SqlLexerInt.parse("\" a b \"")
    assert(toSeq(tokens) ==
      Seq(IDENTIFIER(" a b "))
    )
  }

  test("parse numeric") {
    val r1 = SqlLexerInt.parse(" 12 12. 12.3 .3")

    assert(toSeq(r1) == Seq("12", "12.", "12.3", ".3").map(LITERAL_NUMERIC))
  }

  ignore("parse dummy") {
    var r = SqlLexerInt.parse("insert into t1(f1, f2) values (1, '2')")
    toSeq(r). foreach(println)
  }

  def toSeq(r: Reader[SqlToken]): Seq[SqlToken] = {
    if (r.atEnd) Seq.empty
    else r.first +: toSeq(r.rest)
  }
}

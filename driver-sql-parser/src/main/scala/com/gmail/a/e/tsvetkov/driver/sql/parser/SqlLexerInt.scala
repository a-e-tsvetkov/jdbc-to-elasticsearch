package com.gmail.a.e.tsvetkov.driver.sql.parser

import scala.util.parsing.combinator.RegexParsers

object SqlLexerInt extends RegexParsers {

  import SqlToken._

  override def skipWhitespace: Boolean = true

  val regularIdentifier = "[a-zA-Z][a-zA-Z0-9]*".r ^^ IDENTIFIER
  val delimitedIdentifier = "\"" ~> "[^\"]".r <~ "\"" ^^ { s =>
    IDENTIFIER(s.substring(1, s.length - 1))
  }
  val actualIdentifier = regularIdentifier | delimitedIdentifier


  val unsignedInteger = "[0-9]+".r ^^ (v => LITERAL_INTEGER(Integer.parseInt(v)))
  val literal = unsignedInteger

  val operators =
    "+" ^^ (_ => OP_PLUS) |
      "-" ^^ (_ => OP_MINUS) |
      "*" ^^ (_ => OP_MUL) |
      "/" ^^ (_ => OP_DIV)
  "=" ^^ (_ => OP_EQ) |
    "<>" ^^ (_ => OP_NE) |
    "<" ^^ (_ => OP_LT) |
    ">" ^^ (_ => OP_GT) |
    "<=" ^^ (_ => OP_LE) |
    ">=" ^^ (_ => OP_GE)


  val bracket =
    "(" ^^ (_ => BRACKET_LEFT) |
      ")" ^^ (_ => BRACKET_RIGHT)

  val separator =
    "." ^^ (_ => PERIOD) |
      "," ^^ (_ => COMMA)

  val keyword =
    "create" ^^ (_ => CREATE) |
      "char" ^^ (_ => CHAR) |
      "character" ^^ (_ => CHARACTER) |
      "varchar" ^^ (_ => VARCHAR) |
      "table" ^^ (_ => TABLE) |
      "varying" ^^ (_ => VARYING) |
      "numeric" ^^ (_ => NUMERIC) |
      "decimal" ^^ (_ => DECIMAL) |
      "dec" ^^ (_ => DEC) |
      "smallint" ^^ (_ => SMALLINT) |
      "integer" ^^ (_ => INTEGER) |
      "int" ^^ (_ => INT) |
      "bigint" ^^ (_ => BIGINT) |
      "float" ^^ (_ => FLOAT) |
      "real" ^^ (_ => REAL) |
      "double" ^^ (_ => DOUBLE) |
      "precision" ^^ (_ => PRECISION) |
      "select" ^^ (_ => SELECT) |
      "as" ^^ (_ => AS) |
      "from" ^^ (_ => FROM) |
      "only" ^^ (_ => ONLY) |
      "join" ^^ (_ => JOIN) |
      "inner" ^^ (_ => INNER) |
      "outer" ^^ (_ => OUTER) |
      "left" ^^ (_ => LEFT) |
      "right" ^^ (_ => RIGHT) |
      "full" ^^ (_ => FULL) |
      "on" ^^ (_ => ON) |
      "or" ^^ (_ => OR) |
      "and" ^^ (_ => AND) |
      "true" ^^ (_ => TRUE) |
      "false" ^^ (_ => FALSE) |
      "unknown" ^^ (_ => UNKNOWN) |
      "is" ^^ (_ => IS) |
      "not" ^^ (_ => NOT) |
      "collate" ^^ (_ => COLLATE)

  val goal = rep(keyword
    | actualIdentifier
    | literal
    | bracket
    | separator
  )

  def parse(sqlText: String): Either[SqlParseResultFailure, List[SqlToken]] = {
    parse(goal, sqlText) match {
      case Success(st, next) => Right(st)
      case NoSuccess(msg, next) => Left(SqlParseResultFailure(msg))
    }
  }
}

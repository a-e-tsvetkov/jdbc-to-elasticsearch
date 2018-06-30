package com.gmail.a.e.tsvetkov.driver.sql.parser

import scala.util.parsing.combinator.lexical.{Lexical, Scanners}
import scala.util.parsing.input.CharArrayReader.EofCh
import scala.util.parsing.input.{CharSequenceReader, Reader}

object SqlLexerInt extends Scanners {


  import SqlToken._

  def letter = elem("letter", _.isLetter)

  def digit = elem("digit", _.isDigit)

  def chrExcept(cs: Char*) = elem("![" + cs.mkString("") + "]", ch => !cs.contains(ch))

  def whitespaceChar = elem("space char", ch => ch <= ' ' && ch != EofCh)


  override type Token = SqlToken

  val numericLiteral =
    rep1(digit) ~ opt('.' ~ rep(digit)) ^^ {
      case a ~ b =>
        val txt = b.map(a ++ "." ++ _._2).getOrElse(a)
        LITERAL_NUMERIC(txt.mkString(""))
    } |
      '.' ~> rep1(digit) ^^ {
        x =>
          LITERAL_NUMERIC(('.' +: x).mkString(""))
      }


  val literal = numericLiteral

  val operators =
    '+' ^^^ OP_PLUS |
      '-' ^^^ OP_MINUS |
      '*' ^^^ OP_MUL |
      '/' ^^^ OP_DIV |
      '=' ^^^ OP_EQ |
      '<' ~ '>' ^^^ OP_NE |
      '<' ~ '=' ^^^ OP_LE |
      '<' ^^^ OP_LT |
      '>' ~ '=' ^^^ OP_GE |
      '>' ^^^ OP_GT |
      '|' ~ '|' ^^^ OP_CONCAT


  val bracket =
    '(' ^^^ BRACKET_LEFT |
      ')' ^^^ BRACKET_RIGHT

  val separator =
    '.' ^^^ PERIOD |
      ',' ^^^ COMMA

  val keyword = Map(
    "create" -> CREATE,
    "char" -> CHAR,
    "character" -> CHARACTER,
    "varchar" -> VARCHAR,
    "table" -> TABLE,
    "varying" -> VARYING,
    "numeric" -> NUMERIC,
    "decimal" -> DECIMAL,
    "dec" -> DEC,
    "smallint" -> SMALLINT,
    "integer" -> INTEGER,
    "int" -> INT,
    "bigint" -> BIGINT,
    "float" -> FLOAT,
    "real" -> REAL,
    "double" -> DOUBLE,
    "precision" -> PRECISION,
    "select" -> SELECT,
    "as" -> AS,
    "from" -> FROM,
    "only" -> ONLY,
    "join" -> JOIN,
    "inner" -> INNER,
    "outer" -> OUTER,
    "left" -> LEFT,
    "right" -> RIGHT,
    "full" -> FULL,
    "on" -> ON,
    "or" -> OR,
    "and" -> AND,
    "true" -> TRUE,
    "false" -> FALSE,
    "unknown" -> UNKNOWN,
    "is" -> IS,
    "not" -> NOT,
    "collate" -> COLLATE,
    "insert" -> INSERT,
    "into" -> INTO,
    "values" -> VALUES,
    "where" -> WHERE,
    "order" -> ORDER,
    "by" -> BY,
    "asc" -> ASC,
    "desc" -> DESC
  )

  val identifierOrKeyword = letter ~ rep(letter | digit | '_') ^^ {
    case x ~ xs =>
      val ident = x :: xs mkString ""
      keyword.getOrElse(ident.toLowerCase, IDENTIFIER(ident))
  }
  val delimitedIdentifier =
    ('\"' ~> rep(chrExcept('"')) <~ '"') ^^ { s => IDENTIFIER(s.mkString("")) }

  override def whitespace: Parser[Any] = rep[Any](
    whitespaceChar
      | '-' ~ '-' ~ rep(chrExcept(EofCh, '\n'))
  )


  override def token: Parser[SqlToken] =
    identifierOrKeyword |
      delimitedIdentifier |
      literal |
      operators |
      bracket |
      separator |
      failure("Unknown token")


  override def errorToken(msg: String): SqlToken = ERROR(msg)

  def parse(sqlText: String): Reader[SqlToken] = {
    val reader = new CharSequenceReader(sqlText)
    new Scanner(reader)
  }
}

package com.gmail.a.e.tsvetkov.driver.sql.parser

import com.gmail.a.e.tsvetkov.driver.sql._

import scala.util.parsing.combinator.RegexParsers

object SqlParser {
  def parse(sqlText: String): SqlParseResult = {
    SqlParserInt.parse(sqlText)
  }
}

private object SqlParserInt extends RegexParsers {

  override def skipWhitespace: Boolean = true

  val CREATE = "create"
  val CHAR = "char"
  val CHARACTER = "character"
  val VARCHAR = "varchar"
  val TABLE = "table"
  val VARYING = "varying"
  val NUMERIC = "numeric"
  val DECIMAL = "decimal"
  val DEC = "dec"
  val SMALLINT = "smallint"
  val INTEGER = "integer"
  val INT = "int"
  val BIGINT = "bigint"
  val FLOAT = "float"
  val REAL = "real"
  val DOUBLE = "double"
  val PRECISION = "precision"

  //5.1

  val blahStatement = "blah" ^^ (_ => SqlBlahStatement)
  //  --h3 5.2
  val regularIdentifier = "[a-zA-Z][a-zA-Z0-9]*".r
  val delimitedIdentifier = "\"" ~> "[^\"]".r <~ "\""

  //5.3
  val unsignedInteger = "[0-9]+".r ^^ (v => Integer.parseInt(v))

  //  --h3 5.4
  lazy val columnName = identifier
  lazy val tableName = localOrSchemaQualifiedName
  lazy val localOrSchemaQualifiedName = /*[ <local or schema qualifier> <period> ]*/ qualifiedIdentifier
  val qualifiedIdentifier = identifier
  lazy val identifier = actualIdentifier
  lazy val actualIdentifier = regularIdentifier | delimitedIdentifier
  val collationName = schemaQualifiedName
  lazy val schemaQualifiedName = /*[ <schema name> <period> ]*/ qualifiedIdentifier

  // 6.1
  lazy val dataType: Parser[DataType] = predefinedType
  //    |     <row type>
  //    |     <path-resolved user-defined type name>
  //    |     <reference type>
  //    |     <collection type>

  lazy val predefinedType =
    characterStringType <~
      //      opt("CHARACTER" ~> "SET" ~> characterSetSpecification) <~
      opt(collateClause) |
      //      |     <national character string type> [ <collate clause> ]
      //        |     <binary large object string type>
      numericType //|
  //    booleanType |
  //    datetimeType |
  //    intervalType

  val characterStringTypeOptLength = opt("(" ~> length <~ ")")
  lazy val characterStringType =
    (CHARACTER ~> characterStringTypeOptLength |
      CHAR ~> characterStringTypeOptLength |
      CHARACTER ~> VARYING ~> characterStringTypeOptLength |
      CHAR ~> VARYING ~> characterStringTypeOptLength |
      VARCHAR ~> characterStringTypeOptLength) ^^ (l => DataTypeChar(l))
  //    |     CHARACTER LARGE OBJECT [ <left paren> <large object length> <right paren> ]
  //    |     CHAR LARGE OBJECT [ <left paren> <large object length> <right paren> ]
  //    |     CLOB [ <left paren> <large object length> <right paren> ]
  lazy val length = unsignedInteger

  lazy val numericType = exactNumericType | approximateNumericType


  val exactNumericTypeOptPrecisionScale =
    opt(
      "(" ~>
        precision ~
        opt("," ~> scale) <~
        ")" ^^ (x => DataTypeNumericSpec(x._1, x._2))
    )
  val exactNumericType =
    NUMERIC ~> exactNumericTypeOptPrecisionScale ^^ (s => DataTypeNumeric(s)) |
      DECIMAL ~> exactNumericTypeOptPrecisionScale ^^ (s => DataTypeNumeric(s)) |
      DEC ~> exactNumericTypeOptPrecisionScale ^^ (s => DataTypeNumeric(s)) |
      SMALLINT ^^ (s => DataTypeNumeric(None)) |
      INTEGER ^^ (s => DataTypeNumeric(None)) |
      INT ^^ (s => DataTypeNumeric(None)) |
      BIGINT ^^ (s => DataTypeNumeric(None))

  lazy val precision = unsignedInteger
  lazy val scale = unsignedInteger

  val approximateNumericType =
    FLOAT ~> opt("(" ~> precision <~ ")") ^^ (x => DataTypeFloat(x)) |
      REAL ^^ (_ => DataTypeReal) |
      DOUBLE ~ PRECISION ^^ (_ => DataTypeDouble)

  //10.5
  //  val characterSetSpecification = ???
  //    <standard character set name>
  //  |     <implementation-defined character set name>
  //  |     <user-defined character set name>

  //10.7
  val collateClause = "COLLATE" ~> collationName


  // 11.3
  val tableDefinition =
    (CREATE /* [ tableScope ] */ ~>
      TABLE ~>
      tableName ~
      tableContentsSource) ^^ (x => SqlCreateTableStatement(x._1, x._2))
  /*[ ON COMMIT
         <table commit action>ROWS ] */

  lazy val tableContentsSource = tableElementList
  // | OF   <path-resolved user-defined type name>[     <subtable clause>] [      <table element list>]
  // |        <as subquery clause>
  lazy val tableElementList = "(" ~> rep1sep(tableElement, ",") <~ ")"

  lazy val tableElement = columnDefinition
  // |   <table constraint definition>
  // |    <like clause>
  // |      <self-referencing column specification>
  // |        <column options>

  //11.4
  lazy val columnDefinition = columnName ~ opt(dataType /*|
          <domain name>*/) ^^ (x => ColumnDefenition(x._1, x._2))
  // [   <reference scope check>]
  // [   <default clause>|      <identity column specification>|        <generation clause>]
  // [   <column constraint definition>... ]
  // [   <collate clause>]

  val statement: Parser[SqlStatement] =
    tableDefinition | blahStatement

  val goal: Parser[SqlStatement] = statement

  def parse(sqlText: String): SqlParseResult = {
    val value = parse(goal, sqlText)
    assert(value.next.atEnd)
    value match {
      case Success(st, next) => SqlParseResultSuccess(st)
      case NoSuccess(msg, next) => SqlParseResultFailure(msg)
    }
  }
}

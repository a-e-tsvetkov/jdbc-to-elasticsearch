package com.gmail.a.e.tsvetkov.driver.sql.parser

import com.gmail.a.e.tsvetkov.driver.sql.parser.Util._
import com.gmail.a.e.tsvetkov.driver.sql.{BooleanExpression, _}

import scala.util.parsing.combinator.Parsers

object SqlParser {
  def parse(sqlText: String): SqlParseResult = {
    (for {
      tokens <- SqlLexerInt.parse(sqlText).right
      ast <- SqlParserInt.parse(tokens).right
    } yield ast) match {
      case Left(value) => value
      case Right(value) => SqlParseResultSuccess(value)
    }
  }
}

private object SqlParserInt extends Parsers {

  import SqlToken._

  override type Elem = SqlToken


  //5.3
  lazy val sign = OP_PLUS | OP_MINUS
  lazy val unsignedInteger = {
    accept("<unsigned integer>", { case v: LITERAL_INTEGER => v.value })
  }

  //  --h3 5.4
  lazy val columnName = identifier
  lazy val tableName = localOrSchemaQualifiedName
  lazy val localOrSchemaQualifiedName = /*[ <local or schema qualifier> <period> ]*/ qualifiedIdentifier
  val qualifiedIdentifier = identifier
  lazy val identifier = {
    accept("<identifier>", { case id: IDENTIFIER => id.value })
  }
  val collationName = schemaQualifiedName
  lazy val schemaQualifiedName = /*[ <schema name> <period> ]*/ qualifiedIdentifier
  lazy val queryName = identifier
  lazy val correlationName = identifier

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

  val characterStringTypeOptLength = opt(BRACKET_LEFT ~> length <~ BRACKET_RIGHT)
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
      BRACKET_LEFT ~> precision ~ opt(COMMA ~> scale) <~ BRACKET_RIGHT ^^
        (x => DataTypeNumericSpec(x._1, x._2))
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
    FLOAT ~> opt(BRACKET_LEFT ~> precision <~ BRACKET_RIGHT) ^^ (x => DataTypeFloat(x)) |
      REAL ^^ (_ => DataTypeReal) |
      DOUBLE ~ PRECISION ^^ (_ => DataTypeDouble)

  //6.3
  lazy val valueExpressionPrimary: Parser[ValueExpression] =
    parenthesizedValueExpression |
      nonparenthesizedValueExpressionPrimary
  lazy val parenthesizedValueExpression = BRACKET_LEFT ~> valueExpression <~ BRACKET_RIGHT

  lazy val nonparenthesizedValueExpressionPrimary =
  //    unsignedValueSpecification |
    columnReference |
      //      setFunctionSpecification |
      //      windowFunction |
      //      scalarSubquery |
      //      caseExpression |
      //      castSpecification |
      //      fieldReference |
      //      subtypeTreatment |
      //      methodInvocation |
      //      staticMethodInvocation |
      //      newSpecification |
      //      attributeOrMethodReference |
      //      referenceResolution |
      //      collectionValueConstructor |
      //      arrayElementReference |
      //      multisetElementReference |
      //      routineInvocation |
      //      nextValueExpression |
      failure("unexpected nonparenthesizedValueExpressionPrimary")

  //6.6


  lazy val identifierChain = rep1sep(identifier, PERIOD)

  lazy val basicIdentifierChain = identifierChain

  //6.7
  lazy val columnReference =
    basicIdentifierChain ^^ (ValueExpressionColumnReference(_))
  //      |     MODULE <period> <qualified identifier> <period> <column name>

  //6.25
  lazy val valueExpression =
    commonValueExpression |
      booleanValueExpression |
      rowValueExpression |
      failure("unknown valueExpression")

  lazy val commonValueExpression: Parser[ValueExpression] =
    numericValueExpression |
      stringValueExpression |
      //    datetimeValueExpression |
      //    intervalValueExpression |
      //    userDefinedTypeValueExpression |
      //    referenceValueExpression |
      //    collectionValueExpression |
      failure("unknown commonValueExpression")

  //6.26
  lazy val numericValueExpression: Parser[ValueExpression] =
    term |
      numericValueExpression ~ OP_PLUS ~ term ^^ { case l ~ _ ~ r => NumericExpressionBinary(NumericOperarionPlus, l, r) } |
      numericValueExpression ~ OP_MINUS ~ term ^^ { case l ~ _ ~ r => NumericExpressionBinary(NumericOperarionMinus, l, r) }

  lazy val term: Parser[ValueExpression] =
    factor |
      term ~ OP_MUL ~ factor ^^ { case l ~ _ ~ r => NumericExpressionBinary(NumericOperarionMult, l, r) } |
      term ~ OP_DIV ~ factor ^^ { case l ~ _ ~ r => NumericExpressionBinary(NumericOperarionDiv, l, r) }

  lazy val factor = opt(sign) ~ numericPrimary ^^ {
    case s ~ v => s
      .map(_ => NumericExpressionUnaryMinus(v))
      .getOrElse(v)
  }

  lazy val numericPrimary =
    valueExpressionPrimary
  /*|
       numericValueFunction */

  //6.28
  lazy val stringValueExpression: Parser[ValueExpression] =
    characterValueExpression | blobValueExpression
  lazy val characterValueExpression: Parser[ValueExpression] =
    concatenation | characterFactor
  lazy val concatenation = characterValueExpression ~ OP_CONCAT ~ characterFactor ^^ {
    case l ~ _ ~ r => StringExpressionBinary(StringOperarionConcat, l, r)
  }
  lazy val characterFactor = characterPrimary <~ opt(collateClause)
  lazy val characterPrimary = valueExpressionPrimary | stringValueFunction
  lazy val blobValueExpression: Parser[ValueExpression] =
    blobConcatenation | blobFactor
  lazy val blobFactor = blobPrimary
  lazy val blobPrimary = valueExpressionPrimary | stringValueFunction
  lazy val blobConcatenation = blobValueExpression ~ OP_CONCAT ~ blobFactor ^^ {
    case l ~ _ ~ r => StringExpressionBinary(StringOperarionConcat, l, r)
  }


  //6.29
  lazy val stringValueFunction: Parser[ValueExpression] = //  <character value function> | <blob value function>
    failure("not implemented stringValueFunction")

  //6.34
  lazy val booleanValueExpression: Parser[BooleanExpression] =
    rep1sep(booleanTerm, OR) ^^ { ts => fold1Left[BooleanExpression](ts, (l, r) => BooleanExpressionBinary(BooleanOperarionOr, l, r)) }

  lazy val booleanTerm =
    rep1sep(booleanFactor, AND) ^^ { ts => fold1Left[BooleanExpression](ts, (l, r) => BooleanExpressionBinary(BooleanOperarionAnd, l, r)) }
  lazy val booleanFactor = opt(NOT) ~ booleanTest ^^ { case n ~ e =>
    n.map(_ => BooleanExpressionNot(e))
      .getOrElse(e)
  }
  lazy val booleanTest: Parser[BooleanExpression] = booleanPrimary ~ opt(IS ~> opt(NOT) ~ truthValue) ^^ { case e ~ is =>
    is.map {
      case None ~ true => e
      case None ~ false => BooleanExpressionNot(e)
      case _ ~ true => BooleanExpressionNot(e)
      case _ ~ false => e
    }
      .getOrElse(e)
  }
  lazy val truthValue = TRUE ^^ (_ => true) | FALSE ^^ (_ => false) //| UNKNOWN
  lazy val booleanPrimary = predicate | booleanPredicand
  lazy val booleanPredicand =
    parenthesizedBooleanValueExpression |
      (nonparenthesizedValueExpressionPrimary ^^ BooleanExpressionFromValue)
  lazy val parenthesizedBooleanValueExpression = BRACKET_LEFT ~> booleanValueExpression <~ BRACKET_RIGHT


  //7.1


  lazy val rowValueConstructorPredicand =
    commonValueExpression |
      booleanPredicand
  /*|
       explicitRowValueConstructor*/

  //7.2
  lazy val rowValueExpression =
    rowValueSpecialCase
  /*|
     explicitRowValueConstructor*/
  lazy val rowValuePredicand: Parser[ValueExpression] =
    rowValueSpecialCase |
      rowValueConstructorPredicand
  lazy val rowValueSpecialCase = nonparenthesizedValueExpressionPrimary

  //7.4
  lazy val tableExpression =
    fromClause //~
  //      opt(whereClause) ~
  //      opt(groupByClause) ~
  //      opt(havingClause) ~
  //      opt(windowClause)

  //7.5
  lazy val fromClause = FROM ~> tableReferenceList
  lazy val tableReferenceList = rep1sep(tableReference, COMMA)

  //7.6

  lazy val tableReference = (tablePrimaryOrJoinedTable
    /*~ opt(sampleClause)*/)
  lazy val tablePrimaryOrJoinedTable: Parser[TableReference] = tablePrimary | joinedTable
  lazy val tablePrimary =
    tableOrQueryName ~ opt(opt(AS) ~> correlationName /*opt( "(" ~> derivedColumnList <~ ")")*/) ^^ { case n ~ a => TableReferencePrimary(n, a) } |
      //      derivedTable <~ opt(AS) ~ correlationName /*opt( "(" ~> derivedColumnList <~ ")")*/ |
      //      lateralDerivedTable <~ opt(AS) ~ correlationName /*opt( "(" ~> derivedColumnList <~ ")")*/ |
      //      collectionDerivedTable <~ opt(AS) ~ correlationName /*opt( "(" ~> derivedColumnList <~ ")")*/ |
      //      tableFunctionDerivedTable <~ opt(AS) ~ correlationName /*opt( "(" ~> derivedColumnList <~ ")")*/ |
      //      onlySpec ~ opt(opt(AS) ~> correlationName) /*opt( "(" ~> derivedColumnList <~ ")")*/ |
      //      "(" ~> joinedTable <~ ")"
      failure("unknown tablePrimary")

  lazy val onlySpec = ONLY ~> BRACKET_LEFT ~> tableOrQueryName <~ BRACKET_RIGHT
  lazy val tableOrQueryName = tableName | queryName

  //7.7
  lazy val joinedTable =
  //    crossJoin |
    qualifiedJoin |
      //      naturalJoin |
      //      unionJoin |
      failure("unsupported join")

  lazy val qualifiedJoin =
    tableReference ~
      ((opt(joinType) ^^ (x => x.getOrElse(JoinTypeInner))) <~ JOIN) ~
      tablePrimary ~ // spec tells us that it should be tableReference, but I simplify it here.
      joinSpecification ^^ { case r ~ j ~ t ~ s => TableReferenceJoin(j, r, t, s) }
  lazy val joinSpecification = joinCondition
  /*| namedColumnsJoin*/
  lazy val joinCondition = ON ~> searchCondition
  lazy val joinType = INNER ^^ (_ => JoinTypeInner) |
    outerJoinType <~ opt(OUTER)
  lazy val outerJoinType =
    LEFT ^^ (_ => JoinTypeLeftOuter) |
      RIGHT ^^ (_ => JoinTypeRightOuter) |
      FULL ^^ (_ => JoinTypeFullOuter)

  //7.12
  lazy val querySpecification = (SELECT ~> //[ <set quantifier> ]
    selectList ~ tableExpression) ^^ { case s ~ e => SqlSelectStatement(s, e) }
  lazy val selectList: Parser[Seq[SelectTerm]] =
    OP_MUL ^^ { _ => Seq(SelectTermAll) } |
      rep1sep(selectSublist, COMMA)
  lazy val selectSublist = derivedColumn | qualifiedAsterisk
  lazy val qualifiedAsterisk = asteriskedIdentifierChain <~ PERIOD <~ OP_MUL ^^ { l => SelectTermQualifiedAll(l) }
  //|
  //    allFieldsReference
  lazy val asteriskedIdentifierChain = rep1sep(asteriskedIdentifier, COMMA)
  lazy val asteriskedIdentifier = identifier
  lazy val derivedColumn = valueExpression ~ opt(asClause) ^^ { case v ~ n => SelectTermExpr(v, n) }
  lazy val asClause = opt(AS) ~> columnName

  //8.1
  lazy val predicate: Parser[BooleanExpression] =
    comparisonPredicate |
      //      betweenPredicate |
      //      inPredicate |
      //      likePredicate |
      //      similarPredicate |
      //      nullPredicate |
      //      quantifiedComparisonPredicate |
      //      existsPredicate |
      //      uniquePredicate |
      //      normalizedPredicate |
      //      matchPredicate |
      //      overlapsPredicate |
      //      distinctPredicate |
      //      memberPredicate |
      //      submultisetPredicate |
      //      setPredicate |
      //      typePredicate |
      failure("unknown predicate")

  //8.2
  lazy val comparisonPredicate = rowValuePredicand ~ compOp ~ rowValuePredicand ^^ { case l ~ op ~ r => BooleanExpressionComparision(op, l, r) }
  lazy val compOp =
    OP_EQ ^^ (_ => ComparisionOperarionEq) |
      OP_NE ^^ (_ => ComparisionOperarionNe) |
      OP_LT ^^ (_ => ComparisionOperarionLt) |
      OP_GT ^^ (_ => ComparisionOperarionGt) |
      OP_LE ^^ (_ => ComparisionOperarionLe) |
      OP_GE ^^ (_ => ComparisionOperarionGe) |
      failure("unknown compOp")

  //8.19
  lazy val searchCondition = booleanValueExpression

  //10.5
  //  val characterSetSpecification = ???
  //    <standard character set name>
  //  |     <implementation-defined character set name>
  //  |     <user-defined character set name>

  //10.7
  val collateClause = COLLATE ~> collationName


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
  lazy val tableElementList = BRACKET_LEFT ~> rep1sep(tableElement, COMMA) <~ BRACKET_RIGHT

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
    tableDefinition |
      querySpecification

  val goal: Parser[SqlStatement] = statement

  def parse(tokens: Seq[SqlToken]) = {
    val reader = new TokenReader(tokens)
    val value = goal(reader)
    value match {
      case Success(st, next) => Right(st)
      case NoSuccess(msg, next) => Left(SqlParseResultFailure(msg))
    }
  }
}

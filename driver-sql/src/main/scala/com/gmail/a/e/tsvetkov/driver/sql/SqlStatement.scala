package com.gmail.a.e.tsvetkov.driver.sql

sealed trait SqlStatement

case class SqlCreateTableStatement(name: String, columnDefenition: Seq[ColumnDefenition]) extends SqlStatement

case class SqlSelectStatement(terms: Seq[SelectTerm], from: Seq[TableReference]) extends SqlStatement

case class SqlInsertStatement
(
  tableName: String,
  columns: Option[List[String]],
  sources: List[List[ValueExpression]]
) extends SqlStatement

sealed trait SelectTerm

case object SelectTermAll extends SelectTerm

case class SelectTermExpr(expression: ValueExpression, label: Option[String]) extends SelectTerm

case class SelectTermQualifiedAll(qualifier: Seq[String]) extends SelectTerm


sealed trait TableReference

case class TableReferencePrimary(tableName: String, correlatedName: Option[String]) extends TableReference

case class TableReferenceJoin
(joinType: JoinType,
 ref: TableReference,
 tableReference: TableReferencePrimary,
 clause: AnyRef
) extends TableReference


sealed trait JoinType

case object JoinTypeInner extends JoinType

case object JoinTypeLeftOuter extends JoinType

case object JoinTypeRightOuter extends JoinType

case object JoinTypeFullOuter extends JoinType

sealed trait BooleanExpression extends ValueExpression

case class BooleanExpressionBinary
(operation: BooleanOperarion,
 left: ValueExpression,
 right: ValueExpression
) extends BooleanExpression

case class BooleanExpressionNot(expr: ValueExpression) extends BooleanExpression

case class BooleanExpressionComparision
(operation: ComparisionOperarion,
 left: ValueExpression,
 right: ValueExpression
) extends BooleanExpression

sealed trait BooleanOperarion

case object BooleanOperarionOr extends BooleanOperarion

case object BooleanOperarionAnd extends BooleanOperarion

sealed trait ComparisionOperarion

case object ComparisionOperarionEq extends ComparisionOperarion

case object ComparisionOperarionNe extends ComparisionOperarion

case object ComparisionOperarionGt extends ComparisionOperarion

case object ComparisionOperarionLt extends ComparisionOperarion

case object ComparisionOperarionGe extends ComparisionOperarion

case object ComparisionOperarionLe extends ComparisionOperarion

sealed trait ValueExpressionBinary extends ValueExpression {
  val operation: ValueOperarion
  val left: ValueExpression
  val right: ValueExpression
}

sealed trait ValueOperarion

sealed trait NumericExpression extends ValueExpression

case class NumericExpressionBinary
(operation: NumericOperarion,
 left: ValueExpression,
 right: ValueExpression
) extends NumericExpression with ValueExpressionBinary

case class NumericExpressionUnaryMinus(value: ValueExpression) extends NumericExpression

case class NumericExpressionConstant(value: String) extends NumericExpression

sealed trait NumericOperarion extends ValueOperarion

case object NumericOperarionPlus extends NumericOperarion

case object NumericOperarionMinus extends NumericOperarion

case object NumericOperarionMult extends NumericOperarion

case object NumericOperarionDiv extends NumericOperarion


sealed trait StringExpression extends ValueExpression

case class StringExpressionBinary
(operation: StringOperarion,
 left: ValueExpression,
 right: ValueExpression
) extends StringExpression with ValueExpressionBinary

sealed trait StringOperarion extends ValueOperarion

case object StringOperarionConcat extends StringOperarion


sealed trait ValueExpression

case class ValueExpressionColumnReference(id: SqlIdentifier) extends ValueExpression

case class SqlIdentifier(terms: Seq[String])
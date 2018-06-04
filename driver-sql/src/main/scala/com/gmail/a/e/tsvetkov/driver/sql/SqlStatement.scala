package com.gmail.a.e.tsvetkov.driver.sql

sealed trait SqlStatement

case class SqlCreateTableStatement(name: String, columnDefenition: Seq[ColumnDefenition]) extends SqlStatement

case class SqlSelectStatement(terms: Seq[SelectTerm], rest: AnyRef) extends SqlStatement


sealed trait SelectTerm

object SelectTermAll extends SelectTerm

case class SelectTermExpr(expression: AnyRef, label: Option[String]) extends SelectTerm

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

object JoinTypeInner extends JoinType

object JoinTypeLeftOuter extends JoinType

object JoinTypeRightOuter extends JoinType

object JoinTypeFullOuter extends JoinType

sealed trait BooleanExpression extends ValueExpression

case class BooleanExpressionBinary
(operation: BooleanOperarion,
 left: BooleanExpression,
 right: BooleanExpression
) extends BooleanExpression

case class BooleanExpressionNot(expr: BooleanExpression) extends BooleanExpression

case class BooleanExpressionComparision
(operation: ComparisionOperarion,
 left: ValueExpression,
 right: ValueExpression
) extends BooleanExpression

case class BooleanExpressionFromValue(value: ValueExpression) extends BooleanExpression

sealed trait BooleanOperarion

object BooleanOperarionOr extends BooleanOperarion

object BooleanOperarionAnd extends BooleanOperarion

sealed trait ComparisionOperarion

object ComparisionOperarionEq extends ComparisionOperarion

object ComparisionOperarionNe extends ComparisionOperarion

object ComparisionOperarionGt extends ComparisionOperarion

object ComparisionOperarionLt extends ComparisionOperarion

object ComparisionOperarionGe extends ComparisionOperarion

object ComparisionOperarionLe extends ComparisionOperarion

sealed trait NumericExpression extends ValueExpression

case class NumericExpressionBinary
(operation: NumericOperarion,
 left: NumericExpression,
 right: NumericExpression
) extends NumericExpression

case class NumericExpressionUnaryMinus(value: NumericExpression) extends NumericExpression

case class NumericExpressionFromValue(value: ValueExpression) extends NumericExpression

sealed trait NumericOperarion

object NumericOperarionPlus extends NumericOperarion

object NumericOperarionMinus extends NumericOperarion

object NumericOperarionMult extends NumericOperarion

object NumericOperarionDiv extends NumericOperarion

sealed trait ValueExpression

case class ValueExpressionColumnReference(id: Seq[String]) extends ValueExpression
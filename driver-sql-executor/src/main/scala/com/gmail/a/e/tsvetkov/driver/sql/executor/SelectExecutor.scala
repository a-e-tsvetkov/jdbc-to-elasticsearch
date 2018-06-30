package com.gmail.a.e.tsvetkov.driver.sql.executor

import com.gmail.a.e.tsvetkov.driver.sql._
import com.gmail.a.e.tsvetkov.driver.sql.executor.Util.err
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.searches.queries.QueryDefinition

object SelectExecutor extends Executors {

  object ScopeBuilderFactory extends ScopeBuilderFactory

  object MetadataProvider extends MetadataProvider

  object ValueExtractor extends ValueExtractor

  object JsonSerializer extends JsonValueSerializer

  def generateNameFromExpression(expression: ValueExpression) = {
    expression match {
      case ValueExpressionColumnReference(id) => id.terms.mkString(".")
      case _ => "expr"
    }
  }

  private def columnMetadata(resolver: TypeResolver)(term: SelectTerm) = {
    term match {
      case SelectTermAll => err("Not support * in select")
      case SelectTermExpr(expression, label) =>
        SelectResponseColumnMetadata(
          label.getOrElse(generateNameFromExpression(expression)),
          resolver.resolve(expression)
        )
      case SelectTermQualifiedAll(_) => err("Not support * in select")
    }
  }

  def buildResultSetRow(metadata: SelectResponseMetadata)(fieldsExtractor: ResolvedValueExpression => Value): SelectResponseRow = {
    SelectResponseRow(
      metadata.columns
        .map(x => x.columnExpression)
        .map(fieldsExtractor)
    )
  }

  private def collect(expr: ResolvedValueExpression): Map[ScopeTable, Set[ScopeColumn]] =
    expr match {
      case ResolvedValueExpressionConst(_, _) => Map.empty
      case ResolvedValueExpressionColumnRef(column) => Map((column.table, Set(column)))
      case ResolvedValueExpression1(valueType, operation, sub) =>
        collect(sub)
      case ResolvedValueExpression2(valueType, operation, left, right) =>
        merge(collect(left), collect(right))
    }

  private def collectColumnReference(expressions: Seq[ResolvedValueExpression]) = {
    merge(
      expressions
        .map(collect): _*
    )
  }

  def extractRefAndConst(op: Op2Compare,
                         left: ResolvedValueExpression,
                         right: ResolvedValueExpression) = {
    sealed trait ExprType
    case class Const(c: ResolvedValueExpressionConst) extends ExprType
    case class Ref(r: ResolvedValueExpressionColumnRef) extends ExprType
    case object Other extends ExprType
    def getType(e: ResolvedValueExpression) =
      e match {
        case c: ResolvedValueExpressionConst => Const(c)
        case r: ResolvedValueExpressionColumnRef => Ref(r)
        case _: ResolvedValueExpression1 => Other
        case _: ResolvedValueExpression2 => Other
      }
    (getType(left), getType(right)) match {
      case (Ref(r), Const(c)) => Some((op, r, c))
      case (Const(c), Ref(r)) => Some((op.invert, r, c))
      case _ => None
    }
  }

  def createQuery(expr: TableConstraint): QueryDefinition = expr match {
    case TableConstraint(op, column, value) =>
      op match {
        case OpCompareEq =>
          termQuery(column.name, JsonSerializer.toJsonValue(value))
        case OpCompareNe =>
          rangeQuery(column.name)
            .copy(
              gt = Some(JsonSerializer.toJsonValue(value)),
              lt = Some(JsonSerializer.toJsonValue(value))
            )
        case OpCompareGt =>
          rangeQuery(column.name)
            .copy(gt = Some(JsonSerializer.toJsonValue(value)))
        case OpCompareLt =>
          rangeQuery(column.name)
            .copy(lt = Some(JsonSerializer.toJsonValue(value)))
        case OpCompareGe =>
          rangeQuery(column.name)
            .copy(gte = Some(JsonSerializer.toJsonValue(value)))
        case OpCompareLe =>
          rangeQuery(column.name)
            .copy(lte = Some(JsonSerializer.toJsonValue(value)))
      }
  }

  case class TableConstraint(op: Op2Compare, left: ScopeColumn, right: Value)

  def collectConstraints(expr: ResolvedValueExpression): Seq[TableConstraint] = {
    expr match {
      case ResolvedValueExpressionConst(valueType, value) => Seq.empty
      case ResolvedValueExpressionColumnRef(column) =>
        Seq(TableConstraint(OpCompareEq, column, BooleanValue(true)))
      case ResolvedValueExpression1(valueType, operation, sub) =>
        operation match {
          case OpBooleanNot =>
            sub match {
              case ResolvedValueExpressionColumnRef(column) =>
                Seq(TableConstraint(OpCompareEq, column, BooleanValue(false)))
              case _ => Seq.empty
            }
          case OpUnaryMinus => err("Unexpected")
        }
      case ResolvedValueExpression2(valueType, operation, left, right) =>
        operation match {
          case OpBooleanAnd => collectConstraints(left) ++ collectConstraints(right)
          case OpBooleanOr => Seq.empty
          case op: Op2Compare =>
            extractRefAndConst(op, left, right)
              .map {
                case (o, r, c) =>
                  Seq(TableConstraint(o, r.column, c.value))

              }.toSeq.flatten
          case _: OpNumeric => Seq.empty
          case OpStringConcat => Seq.empty
        }
    }
  }

  def merge(c: Map[ScopeTable, Set[ScopeColumn]]*): Map[ScopeTable, Set[ScopeColumn]] = {
    c.flatMap(_.toSeq)
      .groupBy(_._1)
      .mapValues(_.flatMap(_._2).toSet)
  }

  def queryDataSingle(table: MetadataTable,
                      columns: Seq[ScopeColumn],
                      constraints: Seq[TableConstraint],
                      client: HttpClient): DataSet = {

    val definitions: Seq[QueryDefinition] = constraints.map(createQuery)
    val req = search(table.name)
      .version(false)
      .sourceInclude(columns.map(x => x.name))
      .query(bool(definitions, Seq.empty, Seq.empty))

    val documents = check(client.execute(req).await)

    documents.hits.hits
      .map(_.sourceAsMap)
      .map(m => {
        columns.map(c => {
          (c, ValueExtractor.extractValue(m, c))
        })
      }).toSeq
  }

  type DataSet = Seq[Seq[(ScopeColumn, Value)]]

  def join(joinType: JoinType,
           left: DataSet,
           right: DataSet,
           clause: ResolvedValueExpression): DataSet = {
    assert(joinType == JoinTypeLeftOuter)
    left.flatMap(l => right.map(r => l ++ r))
      .filter(v =>
        ValueExtractor.evaluateExpression(
          v.map(x => (x._1, x._2)).toMap)(
          clause) == BooleanValue(true))
  }

  def queryData(from: QueryPlan,
                dataToExtract: Map[ScopeTable, Set[ScopeColumn]],
                resolver: TypeResolver,
                constraints: Seq[TableConstraint],
                client: HttpClient): DataSet = from match {
    case QuerySimple(table) =>
      val columns = dataToExtract(table)

      assert(constraints.forall(_.left.table == table))

      queryDataSingle(table.table,
        columns.toSeq,
        constraints,
        client)
    case QueryJoin(joinType, ref, tableReference, clause) =>

      val (leftE, rightE) = collectColumnReference(Seq(clause))
        .partition(_._1 != tableReference.table)
      val (leftC, rightC) = constraints
        .partition(_.left.table != tableReference.table)

      val left = queryData(ref, merge(dataToExtract, leftE), resolver, leftC, client)
      val right = queryData(tableReference, merge(dataToExtract, rightE), resolver, rightC, client)
      join(joinType, left, right, clause)
  }

  sealed trait QueryPlan {}

  case class QuerySimple(table: ScopeTable) extends QueryPlan

  case class QueryJoin
  (
    joinType: JoinType,
    leftPlan: QueryPlan,
    rightPlan: QuerySimple,
    clauseResolved: ResolvedValueExpression
  ) extends QueryPlan

  def processFrom(scopeBuilder: ScopeBuilder, x: TableReferencePrimary) = x match {
    case TableReferencePrimary(tableName, correlatedName) =>
      val (scope, table) = scopeBuilder.forTable(tableName, correlatedName.getOrElse(tableName))
      (
        scope,
        QuerySimple(table)
      )
  }

  def processFrom(scopeBuilder: ScopeBuilder, from: TableReference): (Scope, QueryPlan) = from match {
    case x@TableReferencePrimary(tableName, correlatedName) =>
      processFrom(scopeBuilder, x)
    case TableReferenceJoin(joinType, ref, tableReference, clause) =>
      val (leftScope, leftPlan) = processFrom(scopeBuilder, ref)
      val (rightScope, rightPlan) = processFrom(scopeBuilder, tableReference)
      val scope = leftScope.combine(rightScope)
      val clauseResolved = TypeResolver(scope).resolve(clause)
      (
        scope,
        QueryJoin(joinType, leftPlan, rightPlan, clauseResolved)
      )
  }

  def execute(client: HttpClient, s: SqlSelectStatement): SelectResponse = {
    val databaseMetadata = MetadataProvider.getMetadata(client)

    val from = s.from match {
      case t :: Nil => t
      case _ => err("Only select from single table supported")
    }

    val scopeBuilder = ScopeBuilderFactory.scopeBuilder(databaseMetadata)

    val (scope, plan) = processFrom(scopeBuilder, from)

    val resolver = TypeResolver(scope)

    val selectMetadata = buildSelectResponseMetadata(s.terms, resolver)

    val whereExpr = s.where.map(resolver.resolve)

    val columnsToQuery = merge(
      collectColumnReference(
        selectMetadata.columns
          .map(x => x.columnExpression)),
      collectColumnReference(whereExpr.to)
    )

    val data1 = queryData(plan,
      columnsToQuery,
      resolver,
      whereExpr.toSeq.flatMap(collectConstraints),
      client
    )
      .map(r => {
        r.toMap
      })
    val data2 = data1.map(ValueExtractor.evaluateExpression)
      .filter(x => {
        whereExpr.map(y => x(y))
          .getOrElse(BooleanValue(true))
          .getValue[BooleanValue]
      })
      .map(buildResultSetRow(selectMetadata))

    SelectResponse(selectMetadata, data2)
  }

  private def buildSelectResponseMetadata(terms: Seq[SelectTerm], resolver: TypeResolver) = {
    SelectResponseMetadata(terms.map(columnMetadata(resolver)))
  }
}

case class SelectResponseColumnMetadata(name: String, columnExpression: ResolvedValueExpression) {
  val columnType = columnExpression.valueType
}

case class SelectResponseMetadata(columns: Seq[SelectResponseColumnMetadata])

case class SelectResponseRow(values: Seq[Value])

case class SelectResponse
(
  metadata: SelectResponseMetadata,
  data: Seq[SelectResponseRow]
)
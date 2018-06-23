package com.gmail.a.e.tsvetkov.driver.sql.executor

import com.gmail.a.e.tsvetkov.driver.sql._
import com.gmail.a.e.tsvetkov.driver.sql.executor.Util.err
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.search.SearchHit
import com.sksamuel.elastic4s.searches.queries.QueryDefinition

object SelectExecutor extends Executors {

  object ScopeBuilder extends ScopeBuilder

  object MetadataProvider extends MetadataProvider

  object ValueExtractor extends ValueExtractor

  object JsonSerializer extends JsonValueSerializer

  def getTableName(from: Seq[TableReference]) =
    from.head match {
      case TableReferencePrimary(tableName, correlatedName) =>
        tableName
      case TableReferenceJoin(joinType, ref, tableReference, clause) =>
        err("Joins not supported")
    }

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
        val valueExpression = resolver.resolve(expression)
        SelectResponseColumnMetadata(
          label.getOrElse(generateNameFromExpression(expression)),
          valueExpression
        )
      case SelectTermQualifiedAll(qualifier) => err("Not support * in select")
    }
  }

  def readDocument(metadata: SelectResponseMetadata)(h: SearchHit): SelectResponseRow = {
    val fields = h.sourceAsMap
    SelectResponseRow(
      metadata.columns
        .map(x => x.columnExpression)
        .map(ValueExtractor.extract(fields))
    )
  }

  private def collect(expr: ResolvedValueExpression): Seq[(ScopeTable, ScopeColumn)] =
    expr match {
      case ResolvedValueExpressionConst(_, _) => Seq.empty
      case ResolvedValueExpressionColumnRef(table, column) => Seq((table, column))
      case ResolvedValueExpression1(valueType, operation, sub) =>
        collect(sub)
      case ResolvedValueExpression2(valueType, operation, left, right) =>
        collect(left) ++ collect(right)
    }

  private def collectColumnReference(expressions: Seq[ResolvedValueExpression]) = {
    merge(
      expressions
        .flatMap(collect)
        .map(x => (x._1, Set(x._2)))
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

  def createQuery(expr: ResolvedValueExpression): Seq[QueryDefinition] = {
    expr match {
      case ResolvedValueExpressionConst(valueType, value) => Seq.empty
      case ResolvedValueExpressionColumnRef(table, column) =>
        Seq(termQuery(column.name, true))
      case ResolvedValueExpression1(valueType, operation, sub) =>
        operation match {
          case OpBooleanNot =>
            sub match {
              case ResolvedValueExpressionColumnRef(table, column) =>
                Seq(termQuery(column.name, false))
              case _ => Seq.empty
            }
          case OpUnaryMinus => err("Unexpected")
        }
      case expr@ResolvedValueExpression2(valueType, operation, left, right) =>
        operation match {
//          case op: OpBoolean =>
//            op match {
              case OpBooleanAnd => createQuery(left) ++ createQuery(right)
              case OpBooleanOr => Seq.empty
//            }
          case op: Op2Compare =>
            extractRefAndConst(op, left, right)
              .map {
                case (o, r, c) =>
                  Seq(o match {
                    case OpCompareEq =>
                      termQuery(r.column.name, JsonSerializer.toJsonValue(c.value))
                    case OpCompareNe =>
                      rangeQuery(r.column.name)
                        .copy(
                          gt = Some(JsonSerializer.toJsonValue(c.value)),
                          lt = Some(JsonSerializer.toJsonValue(c.value))
                        )
                    case OpCompareGt =>
                      rangeQuery(r.column.name)
                        .copy(gt = Some(JsonSerializer.toJsonValue(c.value)))
                    case OpCompareLt =>
                      rangeQuery(r.column.name)
                        .copy(lt = Some(JsonSerializer.toJsonValue(c.value)))
                    case OpCompareGe =>
                      rangeQuery(r.column.name)
                        .copy(gte = Some(JsonSerializer.toJsonValue(c.value)))
                    case OpCompareLe =>
                      rangeQuery(r.column.name)
                        .copy(lte = Some(JsonSerializer.toJsonValue(c.value)))
                  })
              }.toSeq.flatten
          case _: OpNumeric => Seq.empty
          case OpStringConcat => Seq.empty
        }
    }
  }

  def merge(c: Seq[(ScopeTable, Set[ScopeColumn])]*) = {
    c.flatten
      .groupBy(_._1)
      .mapValues(_.flatMap(_._2).toSet)
      .toSeq
  }

  def execute(client: HttpClient, s: SqlSelectStatement): SelectResponse = {
    val databaseMetadata = MetadataProvider.getMetadata(client)

    val scope = ScopeBuilder.buildScope(databaseMetadata, s.from)

    val resolver = TypeResolver(scope)

    val selectMetadata = buildSelectResponseMetadata(s, resolver)

    val whereExpr = s.where.map(resolver.resolve)

    val columnsToQuery = merge(
      collectColumnReference(
        selectMetadata.columns
          .map(x => x.columnExpression)),
      collectColumnReference(whereExpr.to)
    )


    val table = columnsToQuery.head // Support only one table

    val definitions: Seq[QueryDefinition] = whereExpr.toSeq.flatMap(createQuery)
    val req = search(table._1.name)
      .version(false)
      .sourceInclude(table._2.map(x => x.name))
      .query(bool(definitions, Seq.empty, Seq.empty))

    val documents = check(client.execute(req).await)

    val data = documents.hits.hits.map(readDocument(selectMetadata))

    SelectResponse(selectMetadata, data)
  }

  private def buildSelectResponseMetadata(s: SqlSelectStatement, resolver: TypeResolver) = {
    val termToMetadata = columnMetadata(resolver)(_)
    SelectResponseMetadata(s.terms.map(termToMetadata))
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
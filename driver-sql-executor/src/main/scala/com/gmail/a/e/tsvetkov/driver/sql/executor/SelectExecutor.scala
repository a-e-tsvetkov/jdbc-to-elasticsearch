package com.gmail.a.e.tsvetkov.driver.sql.executor

import com.gmail.a.e.tsvetkov.driver.sql._
import com.gmail.a.e.tsvetkov.driver.sql.executor.Util.err
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.search.SearchHit

object SelectExecutor extends Executors {

  object ScopeBuilder extends ScopeBuilder

  object MetadataProvider extends MetadataProvider

  object ValueExtractor extends ValueExtractor

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


  def columnMetadata(scope: Scope)(term: SelectTerm) = {
    term match {
      case SelectTermAll => err("Not support * in select")
      case SelectTermExpr(expression, label) =>
        val resolver = TypeResolver(scope)
        val valueExpression = resolver.resolve(expression)
        SelectResponseColumnMetadata(
          label.getOrElse(generateNameFromExpression(expression)),
          valueExpression
        )
      case SelectTermQualifiedAll(qualifier) => err("Not support * in select")
    }
  }

  def extractMetadata(databaseMetadata: MetadataDatabase, s: SqlSelectStatement) = {
    val scope = ScopeBuilder.buildScope(databaseMetadata, s.from)
    SelectResponseMetadata(s.terms.map(columnMetadata(scope)))
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

  private def collectColumnReference(selectMetadata: SelectResponseMetadata) = {
    selectMetadata.columns
      .map(x => x.columnExpression)
      .flatMap(collect)
      .groupBy(_._1)
      .mapValues(_.map(_._2))
      .mapValues(_.distinct)
  }

  def execute(client: HttpClient, s: SqlSelectStatement): SelectResponse = {
    val databaseMetadata = MetadataProvider.getMetadata(client)
    val selectMetadata = extractMetadata(databaseMetadata, s)

    val columnsToQuery = collectColumnReference(selectMetadata)
    val table = columnsToQuery.head // Support only one table
    val req = search(table._1.name)
      .version(false)
      .sourceInclude(table._2.map(x => x.name))
    val documents = check(client.execute(req).await)

    val data = documents.hits.hits.map(readDocument(selectMetadata))

    SelectResponse(selectMetadata, data)
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
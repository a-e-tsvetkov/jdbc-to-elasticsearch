package com.gmail.a.e.tsvetkov.driver.sql.executor

import com.gmail.a.e.tsvetkov.driver.sql._
import com.gmail.a.e.tsvetkov.driver.sql.executor.Util.err
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.search.SearchHit

object SelectExecutor extends Executors {

  object ScopeBuilder extends ScopeBuilder
  object MetadataProvider extends MetadataProvider

  def getTableName(from: Seq[TableReference]) =
    from.head match {
      case TableReferencePrimary(tableName, correlatedName) =>
        tableName
      case TableReferenceJoin(joinType, ref, tableReference, clause) =>
        err("Joins not supported")
    }

  def generateNameFromExpression(expression: ValueExpression) = {
    expression match {
      case ValueExpressionColumnReference(id) => id.terms.head
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
          valueExpression.valueType
        )
      case SelectTermQualifiedAll(qualifier) => err("Not support * in select")
    }
  }

  def extractMetadata(databaseMetadata:MetadataDatabase, s: SqlSelectStatement) = {
    val scope = ScopeBuilder.buildScope(databaseMetadata, s.from)
    SelectResponseMetadata(s.terms.map(columnMetadata(scope)))
  }

  def readDocument(metadata: SelectResponseMetadata)(h: SearchHit): SelectResponseRow = {
    SelectResponseRow(
      metadata.columns
        .map(_.name)
        .map(x => h.sourceAsMap(x)))
  }

  def execute(client: HttpClient, s: SqlSelectStatement): SelectResponse = {
    val databaseMetadata = MetadataProvider.getMetadata(client)
    val req = search(getTableName(s.from))
    val selectMetadata = extractMetadata(databaseMetadata, s)
    val documents = check(client.execute(req).await)
    val data = documents.hits.hits.map(readDocument(selectMetadata))
    SelectResponse(selectMetadata, data)
  }

}

case class SelectResponseColumnMetadata(name: String, columnType: ValueType)

case class SelectResponseMetadata(columns: Seq[SelectResponseColumnMetadata])

case class SelectResponseRow(values: Seq[Object])

case class SelectResponse
(
  metadata: SelectResponseMetadata,
  data: Seq[SelectResponseRow]
)
package com.gmail.a.e.tsvetkov.driver.sql.executor

import com.gmail.a.e.tsvetkov.driver.sql._
import com.gmail.a.e.tsvetkov.driver.sql.executor.Util.err
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.search.SearchHit

object SelectExecutor extends Executors {

  def getTableName(from: Seq[TableReference]) =
    from.head match {
      case TableReferencePrimary(tableName, correlatedName) =>
        tableName
      case TableReferenceJoin(joinType, ref, tableReference, clause) =>
        err("Joins not supported")
    }

  def generateNameFromExpression(expression: ValueExpression) = {
    expression match {
      case _: BooleanExpression => err("expression in select is not supported")
      case _: ValueExpressionBinary => err("expression in select is not supported")
      case _: NumericExpression => err("expression in select is not supported")
      case _: StringExpression => err("expression in select is not supported")
      case ValueExpressionColumnReference(id) => id.terms.head
    }
  }

  def termAlias(term: SelectTerm) = {
    term match {
      case SelectTermAll => err("Not support * in select")
      case SelectTermExpr(expression, label) =>
        SelectResponseColumnMetadata(label.getOrElse(generateNameFromExpression(expression)))
      case SelectTermQualifiedAll(qualifier) => err("Not support * in select")
    }
  }

  def extractMetadata(s: SqlSelectStatement) = {
    SelectResponseMetadata(s.terms.map(termAlias))
  }

  def readDocument(metadata: SelectResponseMetadata)(h: SearchHit): SelectResponseRow = {
    SelectResponseRow(
      metadata.columns
        .map(_.name)
        .map(x => h.sourceAsMap(x)))
  }

  def execute(client: HttpClient, s: SqlSelectStatement): SelectResponse = {
    val req = search(getTableName(s.from))
    val metadata = extractMetadata(s)
    val documents = check(client.execute(req).await)
    val data = documents.hits.hits.map(readDocument(metadata))
    SelectResponse(metadata, data)
  }

}

case class SelectResponseColumnMetadata(name: String)

case class SelectResponseMetadata(columns: Seq[SelectResponseColumnMetadata])

case class SelectResponseRow(values: Seq[Object])

case class SelectResponse
(
  metadata: SelectResponseMetadata,
  data: Seq[SelectResponseRow]
)
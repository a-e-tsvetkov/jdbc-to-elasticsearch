package com.gmail.a.e.tsvetkov.driver.sql.executor

import com.gmail.a.e.tsvetkov.driver.sql._
import com.sksamuel.elastic4s.http.ElasticDsl.{bulk, indexInto, _}
import com.sksamuel.elastic4s.http.HttpClient

object InsertExecutor extends Executors with ValueExpressionHandlers {

  def createDoc(columns: Seq[String], values: Seq[ValueExpression]) = {
    columns.zip(values).map {
      case (c, v) => (c, toJsonValue(extractValue(v)))
    }
  }

  def insertReq(tableName: String, columns: Seq[String])(values: Seq[ValueExpression]) = {
    indexInto(tableName, "doc")
      .fields(createDoc(columns, values))

  }

  def execute(client: HttpClient, s: SqlInsertStatement) = {
    val req = bulk(s.sources.map(insertReq(s.tableName, s.columns.get)))
    check(client.execute(req).await)
  }
}

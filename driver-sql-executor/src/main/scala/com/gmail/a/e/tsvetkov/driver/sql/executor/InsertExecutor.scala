package com.gmail.a.e.tsvetkov.driver.sql.executor

import com.gmail.a.e.tsvetkov.driver.sql._
import com.sksamuel.elastic4s.http.ElasticDsl.{bulk, indexInto, _}
import com.sksamuel.elastic4s.http.HttpClient

object InsertExecutor extends Executors {

  object ValueExtractor extends ValueExtractor

  object TypeResolver extends TypeResolver(Scope(Seq()))

  def toJsonValue(value: Value) = {
    value match {
      case BooleanValue(v) => v
      case StringValue(v) => v
      case NumericValue(v) => v.toString
    }
  }

  def createDoc(columns: Seq[String], values: Seq[ResolvedValueExpression]) = {
    columns.zip(values).map {
      case (c, v) => (c, toJsonValue(ValueExtractor.extract(Map())(v)))
    }
  }

  def insertReq(tableName: String, columns: Seq[String])(values: Seq[ValueExpression]) = {
    indexInto(tableName, "doc")
      .fields(
        createDoc(
          columns,
          values.map(TypeResolver.resolve)
        ))

  }

  def execute(client: HttpClient, s: SqlInsertStatement) = {
    val req = bulk(s.sources.map(insertReq(s.tableName, s.columns.get)))
    check(client.execute(req).await)
  }
}

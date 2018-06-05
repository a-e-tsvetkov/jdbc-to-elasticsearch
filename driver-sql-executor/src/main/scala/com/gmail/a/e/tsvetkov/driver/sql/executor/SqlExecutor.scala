package com.gmail.a.e.tsvetkov.driver.sql.executor

import java.sql.SQLException

import com.gmail.a.e.tsvetkov.driver.sql._
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.{HttpClient, RequestFailure}
import com.sksamuel.elastic4s.mappings.FieldDefinition

object SqlExecutor {

  def connect(host: String): HttpClient = {
    val client = HttpClient(ElasticsearchClientUri(host, 9200))
    client.execute(clusterHealth()).await match {
      case Left(value) => throw new SQLException(value.toString)
      case Right(value) =>
    }
    client
  }

  def execute(client: HttpClient, statement: SqlStatement): Unit = {
    statement match {
      case s: SqlCreateTableStatement => CreateTableExecutor.createTable(client, s)
      case s: SqlInsertStatement => InsertExecutor.execute(client, s)
    }
  }
}

trait Util {
  def check[T](value: Either[RequestFailure, T]) = {
    value match {
      case Left(err) => throw new SQLException(err.error.reason)
      case Right(res) => res
    }
  }
}

object CreateTableExecutor extends Util {

  def processType(dataType: DataType): String => FieldDefinition = {
    dataType match {
      case DataTypeChar(length) => keywordField
      case DataTypeNumeric(spec) => intField
      case DataTypeFloat(precission) => floatField
      case DataTypeReal => doubleField
      case DataTypeDouble => doubleField
    }
  }

  def fieldMapping(column: ColumnDefenition) = {
    column.dataType match {
      case Some(dataType) => processType(dataType)(column.name)
      case None => throw new SQLException("colum")
    }
  }

  def createTable(client: HttpClient, statement: SqlCreateTableStatement): Unit = {
    val req = createIndex(statement.name)
      .mappings(
        mapping("doc")
          .fields(statement.columnDefenition.map(fieldMapping))
      )
    check(client.execute(req).await)
  }
}

object InsertExecutor {
  def execute(client: HttpClient, s: SqlInsertStatement): Unit = {

  }
}
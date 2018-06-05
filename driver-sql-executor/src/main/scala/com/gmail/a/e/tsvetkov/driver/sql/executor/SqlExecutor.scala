package com.gmail.a.e.tsvetkov.driver.sql.executor

import java.sql.SQLException

import com.gmail.a.e.tsvetkov.driver.sql._
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient

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
      case s: SqlCreateTableStatement => CreateTableExecutor.execute(client, s)
      case s: SqlInsertStatement => InsertExecutor.execute(client, s)
    }
  }
}

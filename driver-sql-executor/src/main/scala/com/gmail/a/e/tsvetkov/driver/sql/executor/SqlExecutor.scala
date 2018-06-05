package com.gmail.a.e.tsvetkov.driver.sql.executor

import java.sql.SQLException

import com.gmail.a.e.tsvetkov.driver.resultset.{AResultSet, AResultSetBuilder}
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

  def execute(client: HttpClient, statement: SqlStatement): AResultSet = {
    statement match {
      case s: SqlCreateTableStatement =>
        CreateTableExecutor.execute(client, s)
        AResultSetBuilder.builder().build();
      case s: SqlInsertStatement =>
        InsertExecutor.execute(client, s)
        AResultSetBuilder.builder().build();
      case s: SqlSelectStatement =>
        val result = SelectExecutor.execute(client, s)
        val builder = AResultSetBuilder.builder()
        result.metadata.columns.zipWithIndex
          .foreach { case (c, i) => builder.addColumn(i, c.name) }
        result.data
          .foreach { r =>
            val row = builder.addRow()
            r.values.foreach { cell =>
              row.addCell(cell)
            }
          }
        builder.build()
    }
  }
}

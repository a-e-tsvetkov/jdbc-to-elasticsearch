package com.gmail.a.e.tsvetkov.driver.sql.executor

import java.sql.{SQLException, Types}

import com.gmail.a.e.tsvetkov.driver.resultset.{AMetadataColumn, AResultSet, AResultSetBuilder}
import com.gmail.a.e.tsvetkov.driver.sql._
import com.gmail.a.e.tsvetkov.driver.sql.executor.CreateTableExecutor.{check, fieldMapping}
import com.gmail.a.e.tsvetkov.driver.sql.executor.InsertExecutor.createDoc
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient

object SqlExecutor {
  object MetadataExecutor extends MetadataExecutor

  def connect(host: String): HttpClient = {
    val client = HttpClient(ElasticsearchClientUri(host, 9200))
    MetadataExecutor.ensureMetadataIndexExists(client)
    client
  }

  private def fillColumnMetadata(builder: AMetadataColumn.AMetadataColumnBuilder,
                                 c: SelectResponseColumnMetadata) = {
    builder
      .label(c.name)
      .sqlType(c.columnType match {
        case MetadataTypeBoolean => Types.BOOLEAN
        case MetadataTypeChar => Types.VARCHAR
        case MetadataTypeNumeric => Types.NUMERIC
      })
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
        val metadataBuilder = builder.getMetadataBuilder
        result.metadata.columns
          .foreach { c => fillColumnMetadata(metadataBuilder.addColumn(), c) }
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

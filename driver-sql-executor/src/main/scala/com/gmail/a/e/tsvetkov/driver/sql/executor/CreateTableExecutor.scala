package com.gmail.a.e.tsvetkov.driver.sql.executor

import java.sql.SQLException

import com.gmail.a.e.tsvetkov.driver.sql._
import com.sksamuel.elastic4s.http.ElasticDsl.{createIndex, doubleField, floatField, intField, keywordField, mapping}
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.mappings.FieldDefinition
import com.sksamuel.elastic4s.http.ElasticDsl._

object CreateTableExecutor extends Executors with MetadataUpdateExecutor {

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


  def createDatatype(dataType: DataType) = {
    dataType match {
      case DataTypeChar(length) =>
        MetadataTypeChar
      case DataTypeNumeric(spec) =>
        MetadataTypeNumeric
      case DataTypeFloat(precission) =>
        MetadataTypeNumeric
      case DataTypeReal =>
        MetadataTypeNumeric
      case DataTypeDouble =>
        MetadataTypeNumeric
    }
  }

  def createTableMetadata(statement: SqlCreateTableStatement) = {
    MetadataTable(
      statement.name,
      statement.columnDefenition.map {
        case ColumnDefenition(name, dataType) =>
          MetadataColumn(name,
            dataType
              .map(createDatatype)
              .getOrElse(Util.err("Column without "))
          )
      })
  }

  def execute(client: HttpClient, statement: SqlCreateTableStatement): Unit = {
    updateMetadata(client) { metadata =>
      if (metadata.tables.exists(t => t.name == statement.name)) {
        Util.err("Table already exist")
      }
      metadata.copy(tables = metadata.tables :+ createTableMetadata(statement))
    }
    val req = createIndex(statement.name)
      .mappings(
        mapping("doc")
          .fields(statement.columnDefenition.map(fieldMapping))
      )
    check(client.execute(req).await)
  }
}

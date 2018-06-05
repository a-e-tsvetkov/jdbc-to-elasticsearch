package com.gmail.a.e.tsvetkov.driver.sql.executor

import java.sql.SQLException

import com.gmail.a.e.tsvetkov.driver.sql._
import com.sksamuel.elastic4s.http.ElasticDsl.{createIndex, doubleField, floatField, intField, keywordField, mapping}
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.mappings.FieldDefinition
import com.sksamuel.elastic4s.http.ElasticDsl._

object CreateTableExecutor extends Executors {

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

  def execute(client: HttpClient, statement: SqlCreateTableStatement): Unit = {
    val req = createIndex(statement.name)
      .mappings(
        mapping("doc")
          .fields(statement.columnDefenition.map(fieldMapping))
      )
    check(client.execute(req).await)
  }
}

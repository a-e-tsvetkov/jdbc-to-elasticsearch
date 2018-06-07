package com.gmail.a.e.tsvetkov.driver.sql.executor

case class ScopeColumn(private val metadata: MetadataColumn) {
  def name = metadata.name
  def valueType = metadata.columnType
}

case class ScopeTable
(
  name: String,
  alias: String,
  private val tableMetadata: MetadataTable
) {
  def columns = tableMetadata.columns.map(ScopeColumn)

}

case class Scope(tables: Seq[ScopeTable])

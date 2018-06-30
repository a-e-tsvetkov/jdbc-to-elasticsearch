package com.gmail.a.e.tsvetkov.driver.sql.executor

import com.gmail.a.e.tsvetkov.driver.sql.executor.Util.err

case class ScopeColumn
(
  table: ScopeTable,
  private val metadata: MetadataColumn
) {
  def name = metadata.name

  def valueType = metadata.columnType
}

case class ScopeTable
(
  table: MetadataTable,
  alias: String
) {
  def find(columnName: String) =
    tryFind(columnName)
      .getOrElse(err(s"Unable to resolve column name: $columnName"))

  def tryFind(columnName: String) = {
    table.columns
      .find(_.name == columnName)
      .map(ScopeColumn(this, _))
  }

  def name = table.name
}

class Scope(private val tables: Seq[ScopeTable]) {
  def findColumn(columnName: String) =
    tables
      .flatMap(t =>
        t.tryFind(columnName)
      )
      .headOption
      .getOrElse(err(s"Unable to resolve column name: $columnName"))

  def find(tableName: String) =
    tables
      .find(_.alias == tableName)
      .getOrElse(err(s"Unable to resolve table name $tableName"))

  def combine(other: Scope) = new Scope(tables ++ other.tables)
}

object Scope {
  def apply(table: ScopeTable) = new Scope(Seq(table))

  def apply() = new Scope(Seq())
}
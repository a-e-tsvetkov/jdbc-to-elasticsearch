package com.gmail.a.e.tsvetkov.driver.sql.executor

import com.gmail.a.e.tsvetkov.driver.sql.executor.Util.err
import com.gmail.a.e.tsvetkov.driver.sql.{TableReference, TableReferenceJoin, TableReferencePrimary}

trait ScopeBuilderFactory {
  def scopeBuilder(metadata: MetadataDatabase) = {
    new ScopeBuilder(metadata)
  }
}

class ScopeBuilder(metadata: MetadataDatabase) {
  def forTable(tableName: String, correlatedName: String) = {
    val metadataTable = metadata.tables.find(_.name == tableName)
        .getOrElse(err(s"Unable to resolve table $tableName"))
    val table = ScopeTable(metadataTable, correlatedName)
    (Scope(table), table)
  }
}

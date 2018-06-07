package com.gmail.a.e.tsvetkov.driver.sql.executor

import com.gmail.a.e.tsvetkov.driver.sql.executor.Util.err
import com.gmail.a.e.tsvetkov.driver.sql.{TableReference, TableReferenceJoin, TableReferencePrimary}

trait ScopeBuilder {
  def buildScope(metadata: MetadataDatabase, from: Seq[TableReference]) = {
    Scope(from.map(scopeTable(metadata)))
  }

  private def scopeTable(metadata: MetadataDatabase)(t: TableReference) = {
    t match {
      case TableReferencePrimary(tableName, correlatedName) =>
        val tableMetadata = lookupTable(metadata, tableName)
        ScopeTable(tableName, correlatedName.getOrElse(tableName), tableMetadata)
      case TableReferenceJoin(joinType, ref, tableReference, clause) =>
        err("join not supported")
    }
  }

  private def lookupTable(metadata: MetadataDatabase, tableName: String) = {
    metadata.tables.find(t => t.name == tableName)
      .getOrElse {
        err("Table not found " + tableName)
      }
  }

}

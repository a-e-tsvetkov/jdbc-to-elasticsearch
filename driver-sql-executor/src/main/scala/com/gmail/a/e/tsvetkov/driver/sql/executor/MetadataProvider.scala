package com.gmail.a.e.tsvetkov.driver.sql.executor

import com.sksamuel.elastic4s.http.HttpClient

trait MetadataProvider {
  object MetadataExecutor extends MetadataExecutor
  object MetadataUpdateExecutor extends MetadataUpdateExecutor
  def getMetadata(client: HttpClient) = {
    MetadataExecutor.getMetadata(client)._2
  }
}

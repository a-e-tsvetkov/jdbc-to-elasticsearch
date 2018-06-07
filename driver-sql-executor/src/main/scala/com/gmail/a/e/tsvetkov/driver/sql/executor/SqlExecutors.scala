package com.gmail.a.e.tsvetkov.driver.sql.executor

import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.{HttpClient, RequestFailure, RequestSuccess}
import com.sksamuel.elastic4s.jackson.ElasticJackson.Implicits._

trait Executors {
  def check[T](value: Either[RequestFailure, RequestSuccess[T]]) = {
    value match {
      case Left(err) => Util.err(err.error.reason)
      case Right(res) => res.result
    }
  }
}

trait MetadataExecutor {
  private val METADATA_INDEX_NAME = "int_metadata"
  private val METADATA_DOCUMENT_INDEX = "1"

  object Executors extends Executors

  import Executors._

  def getMetadata(client: HttpClient): (Long, MetadataDatabase) = {
    val response = check(
      client.execute(get(METADATA_INDEX_NAME, "doc", METADATA_DOCUMENT_INDEX)
      ).await)
    (response.version,
      response.to[MetadataDatabase])

  }

  def tryUpdate(client: HttpClient, oldVersion: Long, newMetadata: MetadataDatabase) = {
    client.execute(
      indexInto(METADATA_INDEX_NAME, "doc")
        .doc(newMetadata)
        .id(METADATA_DOCUMENT_INDEX)
        .version(oldVersion)
    ).await match {
      case Left(value) => value.error.`type` match {
        case "version_conflict_engine_exception" => false
        case _ => Util.err(value.error.reason)

      }
      case Right(value) => true
    }
  }

  def ensureMetadataIndexExists(client: HttpClient) = {

    val res = check(client.execute(indexExists(METADATA_INDEX_NAME)).await)
    if (!res.exists) {
      check(client.execute(createIndex(METADATA_INDEX_NAME)).await)
      check(client.execute(indexInto(METADATA_INDEX_NAME, "doc")
        .doc(MetadataDatabase())
        .id(METADATA_DOCUMENT_INDEX)
      ).await)

    }
  }

}

trait MetadataUpdateExecutor {

  object MetadataExecutor extends MetadataExecutor

  import MetadataExecutor._


  def updateMetadata(client: HttpClient)(function: MetadataDatabase => MetadataDatabase) = {
    var success = false
    while (!success) {
      val (oldVersion, oldMetadata) = getMetadata(client)
      val newMetadata = function(oldMetadata)
      success = tryUpdate(client, oldVersion, newMetadata)
    }
  }

}

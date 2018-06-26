package com.gmail.a.e.tsvetkov.driver.sql.executor

import java.sql.Types

import com.gmail.a.e.tsvetkov.driver.resultset.{AMetadataColumn, AResultSet, AResultSetBuilder}
import com.gmail.a.e.tsvetkov.driver.sql._
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.ElasticDsl.{clusterState, deleteIndex, _}
import com.sksamuel.elastic4s.http.{HttpClient, RequestFailure}

import scala.concurrent.{ExecutionContext, Future}

object SqlExecutor {
  object MetadataExecutor extends MetadataExecutor

  def connect(host: String): HttpClient = {
    val client = HttpClient(ElasticsearchClientUri(host, 9200))
    MetadataExecutor.ensureMetadataIndexExists(client)
    client
  }

  def deleteAllIndexes(client: HttpClient): Unit = {

    implicit object InPlaceExecutionContext extends ExecutionContext {
      override def execute(runnable: Runnable): Unit = runnable.run()

      override def reportFailure(cause: Throwable): Unit = cause.printStackTrace()
    }


    def unwrapError(message: String)(err: RequestFailure) = {
      Seq(s"$message: ${err.error.reason}")
    }

    def err(value: String) = {
      Seq(value)
    }

    def delInd(indexName: String) = {
      client.execute(deleteIndex(indexName))
        .map(_
          .left.map(unwrapError("Unable to get delete index"))
          .map(r => (indexName, r.result.acknowledged))
        )
    }

    case class Magic[A, B](e: Either[Future[A], Future[B]]) {
      def lift = e.fold(
        l => l.map(Left(_)),
        r => r.map(Right(_))
      )
    }
    import scala.language.implicitConversions

    implicit def eitherExtender[A, B](e: Either[Future[A], Future[B]]) = Magic(e)

    val r = client.execute(clusterState())
      .flatMap(_
        .left.map(unwrapError("Unable to get index list"))
        .flatMap(_.result.metadata.toRight(err("Index list response doesn't have data")))
        .map(_.indices.keySet.toSeq
          .map(delInd))
        .left.map(Future.successful(_))
        .right.map(Future.sequence(_))
        .lift
      )
      .map(_.fold(x => Seq(Left(x)), identity))
    r.await
      .foreach {
        case Left(value) => println("ERROR:" + value)
        case Right(value) => println("Succesfully delete " + value._1 + " " + value._2)
      }


    /*
        client.execute(clusterState())
          .map(_
            .left.map(err("Unable to get index list"))
            .flatMap(_.result.metadata.toRight("Index list response doesn't have data"))
            .map(_.indices.keySet.toSeq
                .map(delInd))
            .map(y => Future.sequence(y))
            .left.map(Future.successful(_).map(Left(_)))
            .map(_.map(Right(_)))
            .fold(identity, identity)
          )
          .flatten
          .map(_.fold(x => Seq(Left(x)), identity))
          .await
          .foreach {
            case Left(value) => println("ERROR:" + value)
            case Right(value) => println("Succesfully delete " + value._1 + " " + value._2)
          }
    */
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
            r.values.foreach {
              case BooleanValue(v) =>
                row.addCell(v)
              case StringValue(v) =>
                row.addCell(v)
              case NumericValue(v) =>
                row.addCell(v.bigDecimal)
            }
          }
        builder.build()
    }
  }
}

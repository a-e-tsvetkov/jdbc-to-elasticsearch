package com.gmail.a.e.tsvetkov.driver.sql.executor

import java.sql.SQLException

import com.sksamuel.elastic4s.http.RequestFailure

trait Executors {
  def check[T](value: Either[RequestFailure, T]) = {
    value match {
      case Left(err) => throw new SQLException(err.error.reason)
      case Right(res) => res
    }
  }
}
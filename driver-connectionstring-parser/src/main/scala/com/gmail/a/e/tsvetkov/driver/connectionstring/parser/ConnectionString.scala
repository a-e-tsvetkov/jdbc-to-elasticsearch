package com.gmail.a.e.tsvetkov.driver.connectionstring.parser

sealed trait ConnectionString {
  def isSupported: Boolean

  def host: String

  def isError: Boolean

  def error: String
}
object ConnectionStringNotSupported extends ConnectionString {
  override def isSupported: Boolean = false

  override def host: String = throw new RuntimeException

  override def isError: Boolean = false

  override def error: String = throw new RuntimeException
}

case class ConnectionStringDetails(host: String) extends ConnectionString {
  override def isSupported: Boolean = true

  override def isError: Boolean = false

  override def error: String = throw new RuntimeException
}

case class ConnectionStringError(error: String) extends ConnectionString {
  override def isSupported: Boolean = true

  override def host: String = throw new RuntimeException

  override def isError: Boolean = true
}


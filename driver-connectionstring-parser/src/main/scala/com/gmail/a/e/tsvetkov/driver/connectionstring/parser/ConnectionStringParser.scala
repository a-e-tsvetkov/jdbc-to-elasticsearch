package com.gmail.a.e.tsvetkov.driver.connectionstring.parser

import scala.util.parsing.combinator.RegexParsers

private object ConnectionStringParserInt extends RegexParsers {

  val host: Parser[String] = "[\\.a-zA-Z0-9\\-]*".r ^^ (x => x)
  val id: Parser[String] = "[a-zA-Z][a-zA-Z0-9_]*".r


  val goal: Parser[(String, String)] = (("jdbc:" ~> id) ~ (":" ~> host)) ^^ (x => (x._1, x._2))

  def parse(text: String): ConnectionString = {
    //noinspection VariablePatternShadow
    parse(goal, text) match {
      case Success((id, host), next) =>
        if (id == "mydriver") {
          ConnectionStringDetails(host)
        } else {
          ConnectionStringNotSupported
        }
      case NoSuccess(msg, next) =>
        ConnectionStringError(msg)
    }
  }
}

object ConnectionStringParser {
  def parse(connectionStringText: String): ConnectionString = {
    ConnectionStringParserInt.parse(connectionStringText)
  }
}

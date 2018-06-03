package com.gmail.a.e.tsvetkov.driver.connectionstring.parser

import org.scalatest.FunSuite


class ConnectionStringParserTest extends FunSuite {

  test("parse unsuported connection string") {
    val result = ConnectionStringParser.parse("jdbc:xxx:abc")
    assert(!result.isSupported)
  }

  test("parse connection string") {
    val result = ConnectionStringParser.parse("jdbc:mydriver:abc")
    assert(result.isSupported)
    assert(!result.isError)
    assert("abc" == result.host)
  }
}

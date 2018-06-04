package com.gmail.a.e.tsvetkov.driver.sql.parser

import scala.util.parsing.input.{NoPosition, Reader}

class TokenReader[T](tokens: Seq[T]) extends Reader[T] {
  override def first = tokens.head

  override def atEnd = tokens.isEmpty

  override def pos = NoPosition

  override def rest = new TokenReader(tokens.tail)
}

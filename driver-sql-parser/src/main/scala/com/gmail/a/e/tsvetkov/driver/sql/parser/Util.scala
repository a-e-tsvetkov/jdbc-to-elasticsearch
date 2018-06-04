package com.gmail.a.e.tsvetkov.driver.sql.parser

object Util {
  def fold1Right[T](ts: Seq[T], op: (T, T) => T): T = {
    ts.tail.foldRight(ts.head)((l, r) => op(l, r))
  }

  def fold1Left[T](ts: Seq[T], op: (T, T) => T): T = {
    ts.tail.foldLeft(ts.head)((l, r) => op(l, r))
  }
}

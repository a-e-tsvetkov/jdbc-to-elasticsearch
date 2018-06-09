package com.gmail.a.e.tsvetkov.driver.sql.executor

import org.scalatest.FunSuite

import scala.reflect.ClassTag

class ValueTest extends FunSuite {

  test("testGetValue") {
    assertX[BooleanValue](BooleanValue, true)
    assertX[StringValue](StringValue, "")
    assertX[NumericValue](NumericValue, 123)
  }

  def assertX[T <: Value : ClassTag](ctor: T#RuntimeType => T, realValue: T#RuntimeType) = {

    val wrapper: Value = ctor(realValue)
    assert(wrapper.getValue[T] == realValue)
  }
}

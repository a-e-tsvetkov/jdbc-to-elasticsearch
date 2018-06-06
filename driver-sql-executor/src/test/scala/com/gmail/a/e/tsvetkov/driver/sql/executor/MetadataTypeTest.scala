package com.gmail.a.e.tsvetkov.driver.sql.executor

import com.gmail.a.e.tsvetkov.driver.sql.executor.MetadataTypeTest_TedtSubject.Test
import com.sksamuel.elastic4s.jackson.JacksonSupport
import org.scalatest.FunSuite

class MetadataTypeTest extends FunSuite {

  test("serialization") {
    val mapper = JacksonSupport.mapper

    Seq(MetadataTypeChar,MetadataTypeNumeric).foreach {t =>
      val src = Test(t)
      val str = mapper.writeValueAsString(src)
      val dst = mapper.readValue[Test](str)
      assert(src == dst)
    }

  }
}
object MetadataTypeTest_TedtSubject{
  case class Test(v: MetadataType)

}
package com.gmail.a.e.tsvetkov.driver.sql.executor

import org.scalatest.PropSpec
import org.scalatest.prop.{PropertyChecks, TableFor1}

class Op2CompareTest extends PropSpec with PropertyChecks {

  val operations: TableFor1[Op2Compare] =
    Table(
      "Op",
      OpCompareEq,
      OpCompareNe,
      OpCompareLe,
      OpCompareGe,
      OpCompareGt,
      OpCompareLt
    )

  val numVals =
    Table(
      "Value",
      NumericValue(1),
      NumericValue(2)
    )

  val strVals =
    Table(
      "Value",
      StringValue("a"),
      StringValue("b")
    )
  val boolVals =
    Table(
      "Value",
      BooleanValue(true),
      BooleanValue(false)
    )

  forAll(operations) { op: Op2Compare =>

    forAll(numVals) { v1 =>
      forAll(numVals) { v2 =>
        assert(op.compute(v1, v2) == op.invert.compute(v2, v1))
      }
    }

    forAll(strVals) { v1 =>
      forAll(strVals) { v2 =>
        assert(op.compute(v1, v2) == op.invert.compute(v2, v1))
      }
    }

    forAll(boolVals) { v1 =>
      forAll(boolVals) { v2 =>
        assert(op.compute(v1, v2) == op.invert.compute(v2, v1))
      }
    }
  }

}
